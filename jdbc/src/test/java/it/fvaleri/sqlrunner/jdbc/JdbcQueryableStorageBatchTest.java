/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.jdbc;

import it.fvaleri.sqlrunner.QueryableStorage;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcQueryableStorageBatchTest {
    @Test
    void shouldBatchWhenBatchSizeIsGreaterThanOne() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeBatch()).thenReturn(new int[]{1, 1});

        Properties queries = new Properties();
        queries.put("write", "insert into test values (?)");
        
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            // First write should return 0 (batch not full yet)
            assertEquals(0, storage.write("write", List.of("foo"), 2));
            // Second write should execute the batch and return 2
            assertEquals(2, storage.write("write", List.of("bar"), 2));
            
            // Verify batch operations were called (addBatch called twice, executeBatch once)
            verify(prepStmt, times(2)).addBatch();
            verify(prepStmt).executeBatch();
        }
    }
    
    @Test
    void shouldExecuteSingleWriteWhenBatchSizeIsOne() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeUpdate()).thenReturn(1);

        Properties queries = new Properties();
        queries.put("write", "insert into test values (?)");
        
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            // Single write should return 1 immediately
            assertEquals(1, storage.write("write", List.of("foo"), 1));
            
            // Verify normal execute was called, not batch
            verify(prepStmt).executeUpdate();
        }
    }
    
    @Test
    void shouldHandleMultipleBatchesIndependently() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeBatch()).thenReturn(new int[]{1, 1});

        Properties queries = new Properties();
        queries.put("write1", "insert into test1 values (?)");
        queries.put("write2", "insert into test2 values (?)");
        
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            // First batch for write1
            assertEquals(0, storage.write("write1", List.of("foo"), 2));
            assertEquals(2, storage.write("write1", List.of("bar"), 2));
            
            // Second batch for write2 (independent)
            assertEquals(0, storage.write("write2", List.of("baz"), 2));
            assertEquals(2, storage.write("write2", List.of("qux"), 2));
        }
    }
    
    @Test
    void shouldHandleLargeBatchSizes() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeBatch()).thenReturn(new int[]{1, 1, 1, 1, 1});

        Properties queries = new Properties();
        queries.put("write", "insert into test values (?)");
        
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            // Build up to batch size of 5
            assertEquals(0, storage.write("write", List.of("1"), 5));
            assertEquals(0, storage.write("write", List.of("2"), 5));
            assertEquals(0, storage.write("write", List.of("3"), 5));
            assertEquals(0, storage.write("write", List.of("4"), 5));
            assertEquals(5, storage.write("write", List.of("5"), 5)); // Should execute batch
        }
    }
}