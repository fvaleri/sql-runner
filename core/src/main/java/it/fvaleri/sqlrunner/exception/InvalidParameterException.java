/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.exception;

/**
 * Exception thrown when invalid parameters are provided.
 */
public class InvalidParameterException extends QueryableStorageException {
    private final String parameterName;

    public InvalidParameterException(String message) {
        super(message, "INVALID_PARAMETER");
        this.parameterName = null;
    }

    public InvalidParameterException(String message, String parameterName) {
        super(message, "INVALID_PARAMETER");
        this.parameterName = parameterName;
    }

    public String getParameterName() {
        return parameterName;
    }
}