package com.victor.console.agent;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.parquet.Strings;
import org.mortbay.util.ajax.JSON;
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
public class HiveClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://localhost:10000/test";
    private static String user = "test";
    private static String password = "test1234";
    private static Map<QueryInstance, HiveStatement> statementMap = new ConcurrentHashMap<>();
    private Connection conn;


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
                if (queryInstance.isOnlyQuery) {
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
                        stmt.executeAsync(sql);
                    }catch (Exception e){
                        statementMap.remove(queryInstance);
                        throw new RuntimeException("hive query failed!");
                    }
                    queryInstance.queryState = QueryState.RUNNING;
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
     * 等待异步执行，获取执行日志
     *
     * @param queryInstance
     * @return
     * @throws Exception
     */
    public QueryInstance waitForOperationToComplete(QueryInstance queryInstance) throws Exception {
        HiveStatement stmt = queryInstance.stmt;
        StringBuilder sb = new StringBuilder();

        boolean isEnd = false;
        int i = 0;
        while (!isEnd) {
            if (queryInstance.queryState == QueryState.CANCELLED) {
                isEnd = true;
            } else {
                Tuple2<String, Boolean> execterLogBean = getExecterLog(stmt, true, isEnd);
                isEnd = execterLogBean._2;
                sb.append(execterLogBean._1);
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
        queryInstance.log = sb.toString();

        if (!Strings.isNullOrEmpty(queryInstance.queryId) && statementMap.containsKey(queryInstance)) {
            statementMap.remove(queryInstance);
        }

        return queryInstance;
    }


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
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }


}
