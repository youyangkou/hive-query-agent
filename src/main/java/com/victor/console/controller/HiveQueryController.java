package com.victor.console.controller;

import com.victor.console.agent.QueryInstance;
import com.victor.console.agent.QueryManager;
import com.victor.console.agent.QueryState;
import com.victor.console.domain.ResponseCode;
import com.victor.console.domain.RestResponse;
import com.victor.console.entity.HiveQueryBean;
import com.victor.console.service.HiveQueryService;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Slf4j
@RestController
@RequestMapping("/queryAgent")
public class HiveQueryController {

    @Autowired
    HiveQueryService hiveQueryService;

    @Autowired
    QueryManager queryManager;

    /**
     * 新增查询
     *
     * @param query_sql
     * @return
     */
    @PostMapping("add")
    public RestResponse add(@ApiParam(value = "query_sql") String query_sql, @ApiParam(value = "project") String project) {
        QueryInstance queryInstance = queryManager.generateQueryBean(project, query_sql, false);

        HiveQueryBean hiveQueryBean = hiveQueryService.get(queryInstance.getQueryId());
        if (hiveQueryBean != null && hiveQueryBean.getQueryState().equals(QueryState.SUCCESS.getQueryState())) {
            return RestResponse.fail("This query has been added,Please do not add it repeatedly ", ResponseCode.CODE_FAIL);
        } else {
            hiveQueryBean = HiveQueryBean.builder()
                    .queryId(queryInstance.getQueryId())
                    .querySql(queryInstance.getQuerySql())
                    .project(queryInstance.getProject())
                    .queryState(QueryState.WAITING.getQueryState())
                    .isOnlyQuery(false)
                    .tmpTable(queryInstance.getTmpTable())
                    .updateTime(new Date(System.currentTimeMillis()))
                    .log("")
                    .build();

            queryManager.addQueryBeanToPendingQueue(queryInstance);
            queryManager.start();
        }

        return RestResponse.success(hiveQueryService.add(hiveQueryBean));
    }


    /**
     * 查询状态
     *
     * @param query_id
     * @return
     */
    @GetMapping("state")
    public RestResponse state(@ApiParam(value = "query_id") String query_id) {
        HiveQueryBean hiveQueryBean = hiveQueryService.get(query_id);
        if (hiveQueryBean != null) {
            return RestResponse.success(hiveQueryBean.getQueryState());
        } else {
            return RestResponse.fail("sorry,this query has no state,please check it!", ResponseCode.CODE_FAIL);
        }
    }


    /**
     * 查询日志
     *
     * @param query_id
     * @return
     */
    @GetMapping("log")
    public RestResponse log(@ApiParam(value = "query_id") String query_id) {
        HiveQueryBean hiveQueryBean = hiveQueryService.get(query_id);
        if (hiveQueryBean != null) {
            String log = hiveQueryBean.getLog();
            if(StringUtils.isEmpty(log)){
               log="There is currently no log generated for the job！";
            }
            return RestResponse.success(log);
        } else {
            return RestResponse.fail("Sorry, there is no such inquiry.!", ResponseCode.CODE_FAIL);
        }
    }


    /**
     * 取消查询
     *
     * @param query_id
     * @return
     */
    @PostMapping("cancel")
    public RestResponse cancel(@ApiParam(value = "query_id") String query_id) {
        HiveQueryBean hiveQueryBean = hiveQueryService.get(query_id);

        if (hiveQueryBean != null
                && !hiveQueryBean.getQueryState().equals(QueryState.SUCCESS.getQueryState())
                && !hiveQueryBean.getQueryState().equals(QueryState.FAILED.getQueryState())
                && !hiveQueryBean.getQueryState().equals(QueryState.CANCELLED.getQueryState())) {

            QueryInstance queryInstance = queryManager.QUERY_MAP.get(hiveQueryBean.getQueryId());
            try {
                queryManager.cancelQuery(queryInstance);
                hiveQueryBean.setQueryState("CANCELLED");
                return RestResponse.success(hiveQueryBean);
            } catch (Exception e) {
                return RestResponse.fail("this query cancel failed!", ResponseCode.CODE_FAIL);
            }
        } else {
            return RestResponse.fail("sorry,this query has been finished,it can't be canceled now!", ResponseCode.CODE_FAIL);
        }
    }


    @GetMapping("select")
    public RestResponse select(@ApiParam(value = "query_id") String query_id) {
        return RestResponse.success(hiveQueryService.get(query_id));
    }


    @PostMapping("delete")
    public RestResponse delete(@ApiParam(value = "query_id") String query_id) {
        HiveQueryBean hiveQueryBeanById = hiveQueryService.get(query_id);
        hiveQueryService.delete(query_id);
        return RestResponse.success(hiveQueryBeanById);
    }


    @PostMapping("update")
    public RestResponse update(@ApiParam(value = "query_id") String query_id, @ApiParam(value = "query_state") String query_state) {
        HiveQueryBean hiveQueryBean = hiveQueryService.get(query_id);
        hiveQueryBean.setQueryState(query_state);
        return RestResponse.success(hiveQueryService.update(hiveQueryBean));
    }

}
