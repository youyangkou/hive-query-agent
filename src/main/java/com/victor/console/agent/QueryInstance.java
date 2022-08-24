package com.victor.console.agent;

import com.google.common.base.Objects;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.apache.hive.jdbc.HiveStatement;


/**
 * @author Gerry
 * @date 2022-08-02
 */
@Data
@Builder
@ToString
public class QueryInstance {

    /**
     * the hashcode of query
     */
    String queryId;

    /**
     * the HiveStatement of this query
     */
    HiveStatement stmt;

    /**
     * the recieve query sql | the ddl of tmp_table
     */
    String querySql;

    /**
     * the db of hive
     */
    String project;

    /**
     * the tmp table
     */
    String tmpTable;

    /**
     * is requeried to create a tmp table
     */
    boolean isOnlyQuery;

    /**
     * the state of query
     */
    QueryState queryState;

    /**
     * the log of query
     */
    String log;

    /**
     * the result of query
     */
    String result;

    /**
     * the timestamp of start execution/second
     */
    long executionTime;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryInstance queryInstance = (QueryInstance) o;
        return isOnlyQuery == queryInstance.isOnlyQuery &&
                Objects.equal(queryId, queryInstance.queryId) &&
                Objects.equal(querySql, queryInstance.querySql) &&
                Objects.equal(project, queryInstance.project) &&
                Objects.equal(tmpTable, queryInstance.tmpTable);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryId, querySql, project, tmpTable, isOnlyQuery);
    }


    public static void main(String[] args) {
        QueryInstance queryInstance = QueryInstance.builder().build();
        queryInstance.queryState = QueryState.SUCCESS;
        System.out.println(queryInstance);
    }


}
