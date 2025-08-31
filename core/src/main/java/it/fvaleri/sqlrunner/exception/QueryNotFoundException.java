/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.exception;

/**
 * Exception thrown when a requested query is not found.
 */
public class QueryNotFoundException extends QueryableStorageException {
    private final String queryName;

    public QueryNotFoundException(String queryName) {
        super(String.format("Query %s not found", queryName), "QUERY_NOT_FOUND");
        this.queryName = queryName;
    }

    public String getQueryName() {
        return queryName;
    }
}