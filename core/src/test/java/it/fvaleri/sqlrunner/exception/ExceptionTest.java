/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionTest {
    @Test
    void shouldCreateQueryableStorageException() {
        QueryableStorageException ex1 = new QueryableStorageException("Test message");
        assertEquals("Test message", ex1.getMessage());
        assertEquals("GENERIC_ERROR", ex1.getErrorCode());
        assertNull(ex1.getCause());

        QueryableStorageException ex2 = new QueryableStorageException("Test message", "CUSTOM_ERROR");
        assertEquals("Test message", ex2.getMessage());
        assertEquals("CUSTOM_ERROR", ex2.getErrorCode());
        assertNull(ex2.getCause());

        RuntimeException cause = new RuntimeException("Root cause");
        QueryableStorageException ex3 = new QueryableStorageException("Test message", cause);
        assertEquals("Test message", ex3.getMessage());
        assertEquals("GENERIC_ERROR", ex3.getErrorCode());
        assertEquals(cause, ex3.getCause());

        QueryableStorageException ex4 = new QueryableStorageException("Test message", "CUSTOM_ERROR", cause);
        assertEquals("Test message", ex4.getMessage());
        assertEquals("CUSTOM_ERROR", ex4.getErrorCode());
        assertEquals(cause, ex4.getCause());
    }

    @Test
    void shouldCreateQueryNotFoundException() {
        QueryNotFoundException ex = new QueryNotFoundException("testQuery");
        assertEquals("Query testQuery not found", ex.getMessage());
        assertEquals("QUERY_NOT_FOUND", ex.getErrorCode());
        assertEquals("testQuery", ex.getQueryName());
    }

    @Test
    void shouldCreateInvalidParameterException() {
        InvalidParameterException ex1 = new InvalidParameterException("Invalid input");
        assertEquals("Invalid input", ex1.getMessage());
        assertEquals("INVALID_PARAMETER", ex1.getErrorCode());
        assertNull(ex1.getParameterName());

        InvalidParameterException ex2 = new InvalidParameterException("Invalid input", "paramName");
        assertEquals("Invalid input", ex2.getMessage());
        assertEquals("INVALID_PARAMETER", ex2.getErrorCode());
        assertEquals("paramName", ex2.getParameterName());
    }

    @Test
    void shouldCreateQueryExecutionException() {
        RuntimeException cause = new RuntimeException("SQL error");
        QueryExecutionException ex = new QueryExecutionException("testQuery", "Execution failed", cause);
        
        assertEquals("Query testQuery failed: Execution failed", ex.getMessage());
        assertEquals("QUERY_EXECUTION_ERROR", ex.getErrorCode());
        assertEquals("testQuery", ex.getQueryName());
        assertEquals(cause, ex.getCause());
    }

    @Test
    void shouldInheritFromQueryableStorageException() {
        assertTrue(QueryableStorageException.class.isAssignableFrom(QueryNotFoundException.class));
        assertTrue(QueryableStorageException.class.isAssignableFrom(InvalidParameterException.class));
        assertTrue(QueryableStorageException.class.isAssignableFrom(QueryExecutionException.class));
    }
}