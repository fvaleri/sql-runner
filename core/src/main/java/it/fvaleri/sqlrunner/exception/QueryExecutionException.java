/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.exception;

/**
 * Exception thrown when query execution fails.
 */
public class QueryExecutionException extends QueryableStorageException {
    private final String queryName;

    public QueryExecutionException(String queryName, String message, Throwable cause) {
        super(String.format("Query %s failed: %s", queryName, message), "QUERY_EXECUTION_ERROR", cause);
        this.queryName = queryName;
    }

    public String getQueryName() {
        return queryName;
    }
}