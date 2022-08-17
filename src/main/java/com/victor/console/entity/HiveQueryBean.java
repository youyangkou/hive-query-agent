package com.victor.console.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Data
@Builder
@TableName("hive_query_bean")
public class HiveQueryBean {

    /**
     * the hashcode of query
     */
    @TableField(value = "query_id")
    String queryId;

    /**
     * the recieve query sql | the ddl of tmp_table
     */
    @TableField(value = "query_sql")
    String querySql;

    /**
     * the db of hive
     */
    String project;

    /**
     * the tmp table
     */
    @TableField(value = "tmp_table")
    String tmpTable;

    /**
     * is requeried to create a tmp table
     */
    @TableField(value = "is_only_query")
    boolean isOnlyQuery;

    /**
     * the state of query
     */
    @TableField(value = "query_state")
    String queryState;

    /**
     * the log of query
     */
    String log;


    @TableField(value = "update_time")
    Date updateTime;


}
