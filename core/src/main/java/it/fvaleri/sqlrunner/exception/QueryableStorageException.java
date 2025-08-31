/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.exception;

/**
 * Base exception for QueryableStorage operations.
 */
public class QueryableStorageException extends RuntimeException {
    private final String errorCode;

    public QueryableStorageException(String message) {
        this(message, "GENERIC_ERROR");
    }

    public QueryableStorageException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public QueryableStorageException(String message, Throwable cause) {
        this(message, "GENERIC_ERROR", cause);
    }

    public QueryableStorageException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}