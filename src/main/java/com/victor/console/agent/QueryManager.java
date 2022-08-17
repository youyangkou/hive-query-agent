package com.victor.console.agent;

import com.victor.console.entity.HiveQueryBean;
import com.victor.console.service.HiveQueryService;
import com.victor.utils.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class QueryManager {

    public static final BlockingQueue<QueryInstance> PENDING_QUEUE = new LinkedBlockingQueue<>();
    public static final Map<String, QueryInstance> QUERY_MAP = new ConcurrentHashMap<>();

    private static AtomicInteger executorThreadNum = new AtomicInteger(0);
    private static int delay = 5;
    private boolean started = false;
    private HiveClient hiveClient = new HiveClient();
    ScheduledExecutorService executorService;

    @Autowired
    HiveQueryService hiveQueryService;

    public QueryManager() {
    }


    public void start() {
        if (started) {
            return;
        }

        log.info("开始创建线程池");
        executorService = Executors.newScheduledThreadPool(5, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName("SQLManager-Thread-" + executorThreadNum.getAndAdd(1));
                return thread;
            }
        });

        log.info("开始定时执行SQL");
        //定时执行sql计算任务
        executorService.scheduleWithFixedDelay(new Runnable() {
                                                   @Override
                                                   public void run() {
                                                       while (PENDING_QUEUE.size() > 0) {
                                                           log.info("PENDING_QUEUE size:" + PENDING_QUEUE.size());

                                                           QueryInstance queryInstance = null;
                                                           String queryId = null;
                                                           try {
                                                               //从堵塞队列中取出一个计算任务
                                                               queryInstance = PENDING_QUEUE.take();
                                                               queryId = queryInstance.getQueryId();
                                                               //异步提交查询到集群
                                                               queryInstance = hiveClient.executeQuery(queryInstance);
                                                               changeHiveQueryState(queryInstance);


                                                               if (!queryInstance.isOnlyQuery) {
                                                                   //等待执行，获取执行日志和执行状态
                                                                   queryInstance = hiveClient.waitForOperationToComplete(queryInstance);
                                                                   changeHiveQueryState(queryInstance);
                                                                   //执行成功，将queryId从QUERY_MAP中删除
                                                                   if (queryInstance.queryState == QueryState.SUCCESS) {
                                                                       QUERY_MAP.remove(queryId);
                                                                   }
                                                               }
                                                           } catch (Exception e) {
                                                               if (queryInstance == null || StringUtils.isEmpty(queryId)) {
                                                                   return;
                                                               }

                                                               queryInstance.queryState = QueryState.FAILED;
                                                               changeHiveQueryState(queryInstance);
                                                               QUERY_MAP.remove(queryId);
                                                           }

                                                           log.info("QUERY_BEAN:" + queryInstance);
                                                       }
                                                       log.info("PENDING_QUEUE size:" + PENDING_QUEUE.size());
                                                   }
                                               },
                0,
                delay,
                TimeUnit.SECONDS);

        started = true;
    }


    /**
     * 线程池停止
     */
    public void stop() {
        if (started) {
            executorService.shutdown();
        }
        started = false;
    }


    /**
     * 将查询加入阻塞队列中
     *
     * @param queryInstance
     */
    public static void addQueryBeanToPendingQueue(QueryInstance queryInstance) {
        String queryId = queryInstance.queryId;
        queryInstance.queryState = QueryState.WAITING;
        QUERY_MAP.put(queryId, queryInstance);
        PENDING_QUEUE.offer(queryInstance);
    }


    /**
     * 取消查询
     *
     * @param queryInstance
     * @return
     * @throws SQLException
     */
    public boolean cancelQuery(QueryInstance queryInstance) throws SQLException {
        boolean result = false;
        //queryState为WAITING的直接从队列中删除即可
        if (queryInstance.queryState == QueryState.WAITING) {
            PENDING_QUEUE.remove(queryInstance);
            QUERY_MAP.remove(queryInstance.queryId);
            result = true;
        } else {
            result = hiveClient.cancelQuery(queryInstance);
        }
        if (result) {
            queryInstance.queryState = QueryState.CANCELLED;
            changeHiveQueryState(queryInstance);
            QUERY_MAP.remove(queryInstance.queryId);
        }
        return result;
    }


    /**
     * 初始化查询对象
     *
     * @param project
     * @param sql
     * @param isOnlyQuery
     * @return
     */
    public static QueryInstance generateQueryBean(String project, String sql, boolean isOnlyQuery) {

        if (org.apache.commons.lang3.StringUtils.isEmpty(project)) {
            project = "default";
        }
        if (org.apache.commons.lang3.StringUtils.isEmpty(sql)) {
            throw new IllegalArgumentException("sql must not be empty");
        }
        if (!sql.toLowerCase().trim().startsWith("select")) {
            throw new IllegalArgumentException(
                    "Prohibit submission of queries that do not start with select");
        }
        if (sql.trim().endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        String queryId = String.valueOf(sql.toLowerCase().replaceAll(" ", "").trim().hashCode());
        String tmpTable = "";

        if (!isOnlyQuery) {
            String ds = TimeUtil.getToday();
            tmpTable = "tmp_" + UUID.randomUUID().toString().replace("-", "_") + "_" + ds;

            StringBuilder ddlSQL = new StringBuilder("Create Table ")
                    .append(project)
                    .append(".")
                    .append(tmpTable)
                    .append(" as ")
                    .append(sql);

            sql = ddlSQL.toString();
        }

        return QueryInstance.builder()
                .project(project)
                .querySql(sql)
                .tmpTable(tmpTable)
                .queryId(queryId)
                .isOnlyQuery(isOnlyQuery)
                .build();
    }


    /**
     * 更新数据库中查询状态和执行日志
     *
     * @param queryInstance
     */
    private void changeHiveQueryState(QueryInstance queryInstance) {
        HiveQueryBean hiveQueryBean = hiveQueryService.get(queryInstance.queryId);
        hiveQueryBean.setQueryState(queryInstance.queryState.getQueryState());
        hiveQueryBean.setLog(queryInstance.log);
        hiveQueryBean.setUpdateTime(new Date(System.currentTimeMillis()));
        hiveQueryService.update(hiveQueryBean);
    }


}


