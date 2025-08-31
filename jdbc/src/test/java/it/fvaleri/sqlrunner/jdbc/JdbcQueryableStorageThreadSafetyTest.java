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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcQueryableStorageThreadSafetyTest {
    @Test
    void shouldHandleConcurrentNormalWrites() throws Exception {
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
            ExecutorService executor = Executors.newFixedThreadPool(10);
            CountDownLatch latch = new CountDownLatch(10);

            // Submit 10 concurrent normal writes
            for (int i = 0; i < 10; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        // Each thread performs multiple normal writes
                        storage.write("write", List.of("value" + threadId));
                        storage.write("write", List.of("another" + threadId));
                        storage.write("write");
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Wait for all threads to complete
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            executor.shutdown();
        }
    }

    @Test
    void shouldHandleConcurrentBatchWrites() throws Exception {
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
            ExecutorService executor = Executors.newFixedThreadPool(10);
            CountDownLatch latch = new CountDownLatch(10);
            
            // Submit 10 concurrent batch writes
            for (int i = 0; i < 10; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        // Each thread performs batch multiple batch writes
                        storage.write("write", List.of("value" + threadId), 2);
                        storage.write("write", List.of("value" + threadId), 2);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            // Wait for all threads to complete
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            executor.shutdown();
        }
    }

    @Test
    void shouldHandleConcurrentReads() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties queries = new Properties();
        queries.put("read", "SELECT * FROM test");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            ExecutorService executor = Executors.newFixedThreadPool(5);
            CountDownLatch latch = new CountDownLatch(5);
            
            // Submit 5 concurrent reads
            for (int i = 0; i < 5; i++) {
                executor.submit(() -> {
                    try {
                        // Each thread performs reads
                        storage.read("read");
                        storage.readSingle("read");
                        storage.readAsStream("read").toList();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            // Wait for all threads to complete
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            executor.shutdown();
        }
    }
}