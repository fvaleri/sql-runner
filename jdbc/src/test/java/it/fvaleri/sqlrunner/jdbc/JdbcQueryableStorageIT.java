/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.jdbc;

import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.StorageConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcQueryableStorageIT {
    static Connection conn;
    static AtomicLong keys;

    @BeforeAll
    static void beforeAll() throws SQLException {
        String url = "jdbc:h2:mem:test;" +
            "INIT=drop table if exists notes\\;" +
            "create table notes(no_id bigint primary key, no_text varchar)";
        conn = DriverManager.getConnection(url);
        keys = new AtomicLong(0);
    }

    @AfterAll
    static void afterAll() throws SQLException {
        conn.close();
    }

    @Test
    void shouldExecuteCrudOperations() throws Exception {
        long key1 = keys.incrementAndGet();
        String value1 = randomUUID().toString();
        long key2 = keys.incrementAndGet();
        String value2 = randomUUID().toString();
        long key3 = keys.incrementAndGet();
        String value3 = randomUUID().toString();

        Properties queries = new Properties();
        queries.put("notes.insert", "insert into notes (no_id, no_text) values (?, ?)");
        queries.put("notes.select.all", "select no_text from notes where no_id in (?, ?, ?)");
        queries.put("notes.select.id", "select no_text from notes where no_id = ?");
        queries.put("notes.select.text", "select no_id from notes where no_text = ?");
        queries.put("notes.update", "update notes set no_text = ? where no_id = ?");
        queries.put("notes.delete", "delete from notes where no_id = ?");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            // insert some data
            assertEquals(1, storage.write("notes.insert", List.of(key1, value1)));
            assertEquals(1, storage.write("notes.insert", List.of(key2, value2)));
            assertEquals(1, storage.write("notes.insert", List.of(key3, value3)));

            // check they are there
            List<String> result1 = new ArrayList<>();
            List<List<Object>> rows1 = storage.read("notes.select.all", List.of(key1, key2, key3));
            rows1.forEach(row -> result1.add((String) row.get(0)));
            assertTrue(result1.containsAll(List.of(value1, value2, value3)));

            // update one row
            assertEquals(1, storage.write("notes.update", List.of(value3 + "*", key3)));

            // check it was updated
            assertEquals(value3 + "*", storage.read("notes.select.id", List.of(key3)).get(0).get(0));
            assertEquals(key3, storage.read("notes.select.text", List.of(value3 + "*")).get(0).get(0));

            // delete one row
            assertEquals(1, storage.write("notes.delete", List.of(key2)));

            // check it was deleted
            assertTrue(storage.read("notes.select.id", List.of(key2)).isEmpty());
        }
    }

    @Test
    void shouldFailWithInvalidQueryParams() throws Exception {
        long key = keys.incrementAndGet();

        Properties queries = new Properties();
        queries.put("notes.insert", "insert into notes (no_id, no_text) values (?, ?)");
        queries.put("notes.select", "select no_text from notes where no_id = ? and no_text = ?");
        queries.put("notes.update", "update notes set no_text = ? where no_id = ? and no_text = ?");
        queries.put("notes.delete", "delete from notes where no_id = ? and no_text = ?");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Exception e1 = assertThrows(RuntimeException.class, () -> storage.write("notes.insert", null));
            assertTrue(e1.getMessage().contains("Query notes.insert failed"));

            Exception e2 = assertThrows(RuntimeException.class, () -> storage.write("notes.insert", List.of()));
            assertTrue(e2.getMessage().contains("Query notes.insert failed"));

            Exception e3 = assertThrows(RuntimeException.class, () -> storage.write("notes.insert", List.of(key)));
            assertTrue(e3.getMessage().contains("Query notes.insert failed"));

            Exception e4 = assertThrows(RuntimeException.class, () -> storage.read("notes.select", null));
            assertTrue(e4.getMessage().contains("Query notes.select failed"));

            Exception e5 = assertThrows(RuntimeException.class, () -> storage.read("notes.select"));
            assertTrue(e5.getMessage().contains("Query notes.select failed"));

            Exception e6 = assertThrows(RuntimeException.class, () -> storage.write("notes.update", null));
            assertTrue(e6.getMessage().contains("Query notes.update failed"));

            Exception e7 = assertThrows(RuntimeException.class, () -> storage.write("notes.update", List.of()));
            assertTrue(e7.getMessage().contains("Query notes.update failed"));

            Exception e8 = assertThrows(RuntimeException.class, () -> storage.write("notes.update", List.of(key)));
            assertTrue(e8.getMessage().contains("Query notes.update failed"));

            Exception e9 = assertThrows(RuntimeException.class, () -> storage.write("notes.delete", null));
            assertTrue(e9.getMessage().contains("Query notes.delete failed"));

            Exception e10 = assertThrows(RuntimeException.class, () -> storage.write("notes.delete", List.of()));
            assertTrue(e10.getMessage().contains("Query notes.delete failed"));

            Exception e11 = assertThrows(RuntimeException.class, () -> storage.write("notes.delete", List.of(key)));
            assertTrue(e11.getMessage().contains("Query notes.delete failed"));
        }
    }

    @Test
    void shouldWriteAndReadNullParameter() throws Exception {
        long key = keys.incrementAndGet();
        String value = randomUUID().toString();

        Properties queries = new Properties();
        queries.put("notes.insert", "insert into notes (no_id, no_text) values (?, ?)");
        queries.put("notes.select", "select no_text from notes where no_id = ?");
        queries.put("notes.update", "update notes set no_text = ? where no_id = ?");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            assertEquals(1, storage.write("notes.insert", List.of(key, value)));
            assertEquals(value, storage.read("notes.select", List.of(key)).get(0).get(0));

            assertEquals(1, storage.write("notes.update", Arrays.asList(null, key)));
            assertNull(storage.read("notes.select", List.of(key)).get(0).get(0));
        }
    }

    @Test
    void shouldFailWithInvalidQuery() {
        Properties queries1 = new Properties();
        queries1.put("notes.insert.invalid", "insert into foo (id, name) values (?, ?)");
        Exception e1 = assertThrows(RuntimeException.class, () -> JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries1)
                .build());
        assertTrue(e1.getMessage().contains("Init error"));

        Properties queries2 = new Properties();
        queries2.put("notes.select.invalid", "select value from notes where no_id = ?");
        Exception e2 = assertThrows(RuntimeException.class, () -> JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries2)
                .build());
        assertTrue(e2.getMessage().contains("Init error"));
    }

    @Test
    void shouldExecuteBatchWrites() throws Exception {
        int numOfBatches = 2;
        int batchSize = 100;

        for (int i = 0; i < numOfBatches; i++) {
            List<Long> batchKeys = IntStream.range(0, batchSize)
                .mapToObj(n -> keys.incrementAndGet()).collect(Collectors.toList());
            List<String> batchValues = IntStream.range(0, batchSize)
                .mapToObj(n -> randomUUID().toString()).collect(Collectors.toList());

            Properties queries = new Properties();
            queries.put("notes.insert", "insert into notes (no_id, no_text) values (?, ?)");

            try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
                for (int j = 0; j < batchSize - 1; j++) {
                    assertEquals(0, storage.write("notes.insert", List.of(batchKeys.get(j), batchValues.get(j)), batchSize));
                }
                assertEquals(batchSize, storage.write("notes.insert", List.of(batchKeys.get(batchSize - 1), batchValues.get(batchSize - 1)), batchSize));
            }
        }
    }

    @Test
    void shouldHandleTransactionCommit() throws Exception {
        long key1 = keys.incrementAndGet();
        String value1 = randomUUID().toString();
        long key2 = keys.incrementAndGet();
        String value2 = randomUUID().toString();

        Properties queries = new Properties();
        queries.put("notes.insert", "insert into notes (no_id, no_text) values (?, ?)");
        queries.put("notes.select", "select no_text from notes where no_id = ?");
        queries.put("notes.count", "select count(*) from notes where no_id in (?, ?)");
        StorageConfig config = StorageConfig.builder().autoCommit(false).build();

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .config(config)
                .build()) {
            
            assertEquals(1, storage.write("notes.insert", List.of(key1, value1)));
            assertEquals(1, storage.write("notes.insert", List.of(key2, value2)));
            
            storage.commit();
            
            assertEquals(value1, storage.read("notes.select", List.of(key1)).get(0).get(0));
            assertEquals(value2, storage.read("notes.select", List.of(key2)).get(0).get(0));
            assertEquals(2L, storage.read("notes.count", List.of(key1, key2)).get(0).get(0));
        }
    }

    @Test
    void shouldHandleTransactionRollback() throws Exception {
        long key1 = keys.incrementAndGet();
        String value1 = randomUUID().toString();
        long key2 = keys.incrementAndGet();
        String value2 = randomUUID().toString();

        Properties queries = new Properties();
        queries.put("notes.insert", "insert into notes (no_id, no_text) values (?, ?)");
        queries.put("notes.select", "select no_text from notes where no_id = ?");
        queries.put("notes.count", "select count(*) from notes where no_id in (?, ?)");

        String url = "jdbc:h2:mem:rollback_test;" +
            "INIT=drop table if exists notes\\;" +
            "create table notes(no_id bigint primary key, no_text varchar)";
        
        try (Connection rollbackConn = DriverManager.getConnection(url)) {
            StorageConfig config = StorageConfig.builder().autoCommit(false).build();
            
            try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(rollbackConn)
                .queries(queries)
                .config(config)
                .build()) {
            
                assertEquals(1, storage.write("notes.insert", List.of(key1, value1)));
                assertEquals(1, storage.write("notes.insert", List.of(key2, value2)));
                
                // Verify data exists before rollback
                assertEquals(2L, storage.read("notes.count", List.of(key1, key2)).get(0).get(0));
                
                storage.rollback();
                
                // After rollback, data should be gone
                assertEquals(0L, storage.read("notes.count", List.of(key1, key2)).get(0).get(0));
            }
        }
    }
}
