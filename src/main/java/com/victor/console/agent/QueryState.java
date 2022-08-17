package com.victor.console.agent;

/**
 * @author Gerry
 * @date 2022-08-11
 */
public enum QueryState {

    WAITING("WAITING"),
    RUNNING("RUNNING"),
    SUCCESS("SUCCESS"),
    FAILED("FAILED"),
    CANCELLED("CANCELLED");

    private String value;

    QueryState(String value) {
        this.value = value;
    }

    public String getQueryState() {
        return value;
    }
}
