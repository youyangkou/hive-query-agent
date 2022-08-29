package com.victor.console.agent;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.victor.console.conf.HiveConfigProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.parquet.Strings;
import org.mortbay.util.ajax.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Gerry
 * @date 2022-08-08
 */
@Slf4j
@Component
public class HiveClient {

    private static Map<QueryInstance, HiveStatement> statementMap = new ConcurrentHashMap<>();
    private Connection conn;

    @Autowired
    HiveConfigProperties hiveConfigProperties;


    /**
     * 执行查询
     *
     * @param queryInstance 封装的查询实体
     * @return QueryInstance
     */
    public QueryInstance executeQuery(QueryInstance queryInstance) throws ClassNotFoundException, SQLException {
        String queryId = queryInstance.getQueryId();
        String sql = queryInstance.getQuerySql();

        if (conn == null) {
            conn = getConn();
        }

        HiveStatement stmt;
        synchronized (statementMap) {
            //检查该query是否正在运行
            this.checkRunningQueue(queryId);
            stmt = (HiveStatement) conn.createStatement();
            queryInstance.stmt = stmt;

            if (!StringUtils.isEmpty(queryId)) {
                queryInstance.setExecutionTime(System.currentTimeMillis() / 1000);
                String useDBsql = new StringBuilder("use ").append(queryInstance.project).toString();
                String yarnQueueSql = new StringBuilder("set mapreduce.job.queuename=").append(hiveConfigProperties.getYarnQueuename()).toString();
                stmt.execute(yarnQueueSql);
                stmt.execute(useDBsql);
                if (queryInstance.isOnlyQuery) {
                    log.info("SQL:{}，是只查询SQL，不创建临时表", queryInstance.querySql);
                    try {
                        queryInstance.result = parseResultSet(stmt.executeQuery(sql));
                        queryInstance.queryState = QueryState.SUCCESS;
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                        if (!Strings.isNullOrEmpty(queryId) && statementMap.containsKey(queryInstance)) {
                            statementMap.remove(queryInstance);
                        }
                    }
                } else {
                    //需要异步执行的query,缓存其HiveStatement对象,以提供cancel的功能
                    statementMap.put(queryInstance, stmt);
                    try {
                        log.info("SQL:{}，开始异步执行，创建临时表{}", queryInstance.querySql, queryInstance.tmpTable);
                        stmt.executeAsync(sql);
                        queryInstance.queryState = QueryState.RUNNING;
                        log.info("查询已异步提交，query_id={},queryInstance={}", queryId, queryInstance);
                    } catch (Exception e) {
                        statementMap.remove(queryInstance);
                        log.error("SQL:{}，异步执行发生异常", queryInstance.querySql);
                        throw new RuntimeException("hive query failed!");
                    }
                }
            }
        }
        return queryInstance;
    }


    /**
     * 取消查询
     *
     * @param queryInstance
     * @return
     * @throws SQLException
     */
    public boolean cancelQuery(QueryInstance queryInstance) throws SQLException {
        String queryId = queryInstance.getQueryId();
        boolean isSuccess = false;
        HiveStatement stmt = null;

        synchronized (statementMap) {
            //判断statementMap是否包含queryId
            if (!statementMap.containsKey(queryInstance)) {
                throw new RuntimeException(String.format("sql [%s] has been canceled and clear !", queryId));
            } else {
                try {
                    stmt = statementMap.get(queryInstance);
                    log.info("开始取消查询,query_sql={}", queryInstance.querySql);
                    stmt.cancel();
                    isSuccess = true;
                    queryInstance.queryState = QueryState.CANCELLED;
                    log.info("取消查询成功,query_sql={}", queryInstance.querySql);
                } catch (Exception e) {
                    queryInstance.queryState = QueryState.FAILED;
                    log.info("取消查询失败,query_sql={}", queryInstance.querySql);
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (!Strings.isNullOrEmpty(queryId) && statementMap.containsKey(queryInstance)) {
                        statementMap.remove(queryInstance);
                    }
                }
            }
        }
        return isSuccess;
    }


    /**
     * 等待异步执行，执行完成获取执行日志
     *
     * @param queryInstance
     * @return
     * @throws Exception
     */
    public QueryInstance waitForOperationToComplete(QueryInstance queryInstance) throws Exception {
        log.info("SQL异步执行中,SQL:{}", queryInstance.querySql);
        HiveStatement stmt = queryInstance.getStmt();
        StringBuilder sb = new StringBuilder();
        if (stmt != null) {
            boolean isEnd = false;
            int i = 0;
            while (!isEnd) {
                if (queryInstance.queryState == QueryState.CANCELLED) {
                    isEnd = true;
                } else {
                    Tuple2<String, Boolean> execterLogBean = getExecterLog(stmt, true, isEnd);
                    isEnd = execterLogBean._2;
                    sb.append(execterLogBean._1);
                    queryInstance.log = sb.toString();
                    if (isEnd) queryInstance.queryState = QueryState.SUCCESS;
                }
                i++;
                if (i > 1000) {
                    log.info("查询超时被关闭,query_sql={}", queryInstance.querySql);
                    isEnd = true;
                    stmt.cancel();
                    stmt.close();
                    queryInstance.queryState = QueryState.FAILED;
                }
                Thread.sleep(5000);
            }


            if (!Strings.isNullOrEmpty(queryInstance.queryId) && statementMap.containsKey(queryInstance)) {
                statementMap.remove(queryInstance);
            }
        } else {
            log.error("sql {} stmt has been close!", queryInstance.getQueryId());
            throw new RuntimeException(String.format("sql [%s] stmt has been close !", queryInstance.getQueryId()));
        }
        return queryInstance;
    }


    /**
     * 获取查询日志，适合配合waitForOperationToComplete方法使用，日志获取完成才会返回给客户端
     *
     * @param queryInstance
     * @param incremental
     * @return
     * @throws SQLException
     */
    private QueryInstance getExecterLog(QueryInstance queryInstance, boolean incremental) throws SQLException {
        HiveStatement stmt = queryInstance.getStmt();
        List<String> queryLog = stmt.getQueryLog(incremental, 100);
        if (queryLog.size() > 0) {
            String log = queryInstance.log == null ? "" : queryInstance.log;
            StringBuilder sb = new StringBuilder(log);
            for (String logItem : queryLog) {
                sb.append(logItem + "\n");
                queryInstance.log = sb.toString();
                if (logItem.contains("INFO  : OK")) {
                    queryInstance.queryState = QueryState.SUCCESS;
                } else if (logItem.contains("ERROR") || logItem.contains("Exception")) {
                    queryInstance.queryState = QueryState.FAILED;
                }
            }
        }
        return queryInstance;
    }


    /**
     * 等待异步执行，客户端使用while循环调用
     *
     * @param queryInstance
     * @return
     * @throws Exception
     */
    public QueryInstance waitExecutionForComplete(QueryInstance queryInstance) throws Exception {
        log.info("SQL异步执行中,SQL:{}", queryInstance.querySql);
        HiveStatement stmt = queryInstance.getStmt();
        if (stmt != null) {
            queryInstance = getExecterLog(queryInstance, true);
            if (!Strings.isNullOrEmpty(queryInstance.queryId) && statementMap.containsKey(queryInstance)) {
                if (queryInstance.queryState == QueryState.SUCCESS || queryInstance.queryState == QueryState.FAILED)
                    statementMap.remove(queryInstance);
            }
        } else {
            log.error("sql {} stmt has been close!", queryInstance.getQueryId());
            throw new RuntimeException(String.format("sql [%s] stmt has been close !", queryInstance.getQueryId()));
        }
        Thread.sleep(5000);
        return queryInstance;
    }


    /**
     * 获取查询日志，适合配合waitExecutionForComplete使用，客户端使用while循环可以不断获取日志输出
     *
     * @param stmt
     * @param incremental
     * @param isEnd
     * @return
     * @throws SQLException
     */
    private Tuple2<String, Boolean> getExecterLog(HiveStatement stmt, boolean incremental, boolean isEnd) throws SQLException {
        StringBuilder sb = new StringBuilder();
        List<String> queryLog = stmt.getQueryLog(incremental, 100);
        if (queryLog.size() > 0) {
            for (String logItem : queryLog) {
                sb.append(logItem + "\n");
                if (logItem.contains("INFO  : OK")) {
                    isEnd = true;
                }
            }
        }
        return Tuple2.apply(sb.toString(), isEnd);
    }


    /**
     * 检查该queryId对应的hql是否正在执行
     *
     * @param queryId
     */
    private void checkRunningQueue(String queryId) {
        if (Strings.isNullOrEmpty(queryId)) return;

        Set<QueryInstance> sets = getQueryBeansInRunningQueueByQueryId(queryId);
        if (sets.isEmpty()) return;

        throw new RuntimeException(String.format("sql [%s] has in the running queue,please wait!", queryId));
    }


    private static Set<QueryInstance> getQueryBeansInRunningQueueByQueryId(String queryId) {
        return Sets.filter(statementMap.keySet(), new Predicate<QueryInstance>() {
            @Override
            public boolean apply(@Nullable QueryInstance queryInstance) {
                assert queryInstance != null;
                return queryInstance.getQueryId().equals(queryId);
            }
        });
    }


    /**
     * 解析Hive查询返回值，封装为JSON返回
     *
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private String parseResultSet(ResultSet resultSet) throws SQLException {
        String resultStr;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int colCount = metaData.getColumnCount();
        List res = Lists.newArrayList();
        while (resultSet.next()) {
            List row = Lists.newArrayList();
            for (int i = 1; i <= colCount; i++) {
                row.add(resultSet.getObject(i));
            }
            res.addAll(row);
        }
        resultStr = JSON.toString(res);
        return resultStr;
    }


    private Connection getConn() throws ClassNotFoundException, SQLException {
        if (conn == null) {
            Class.forName(hiveConfigProperties.getDriverName());
            conn = DriverManager.getConnection(hiveConfigProperties.getUrl(), hiveConfigProperties.getUser(), hiveConfigProperties.getPassWord());
        }
        return conn;
    }


}
