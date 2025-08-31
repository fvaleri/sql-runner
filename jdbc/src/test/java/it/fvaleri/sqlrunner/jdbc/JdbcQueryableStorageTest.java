/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.jdbc;

import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.StorageConfig;
import it.fvaleri.sqlrunner.exception.InvalidParameterException;
import it.fvaleri.sqlrunner.exception.QueryNotFoundException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcQueryableStorageTest {
    @Test
    void shouldWriteUsingExistingQuery() throws Exception {
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
            assertEquals(1, storage.write("write", List.of("foo", "bar")));
        }
    }

    @Test
    void shouldReadUsingExistingQueryWithParams() throws Exception {
        List<String> data = List.of("v1", "v2", "v3");

        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(anyInt())).thenReturn(data.get(1));

        Properties queries = new Properties();
        queries.put("read", "select 1 from test");
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            assertEquals("v2", storage.read("read", List.of("k2")).get(0).get(0));
        }
    }

    @Test
    void shouldReadUsingExistingQueryWithoutParams() throws Exception {
        List<String> data = List.of("v1", "v2", "v3");

        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(anyInt())).thenReturn(data.get(0)).thenReturn(data.get(1)).thenReturn(data.get(2));

        Properties queries = new Properties();
        queries.put("read", "select 1 from test");
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            List<List<Object>> rows = storage.read("read");
            List<String> result = rows.stream().map(row -> (String) row.get(0)).collect(Collectors.toList());
            assertEquals(data, result);
        }
    }

    @Test
    void shouldReturnEmptyListWhenNoResult() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(false);

        Properties queries = new Properties();
        queries.put("read", "select 1 from test");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            List<List<Object>> rows = storage.read("read");
            assertTrue(rows.isEmpty());
        }
    }

    @Test
    void shouldFailWhenConnectionIsNullOrClosed() throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties queries = new Properties();
        queries.put("read", "select 1 from test");

        Exception e1 = assertThrows(InvalidParameterException.class, () -> 
                JdbcQueryableStorage.builder().connection(null).queries(queries).build());
        assertEquals("Connection is required", e1.getMessage());

        when(conn.isClosed()).thenReturn(true);
        Exception e2 = assertThrows(InvalidParameterException.class, () -> 
                JdbcQueryableStorage.builder().connection(conn).queries(queries).build());
        assertEquals("Invalid connection", e2.getMessage());
    }

    @Test
    void shouldFailWhenQueriesAreNullOrEmpty() throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Exception e1 = assertThrows(InvalidParameterException.class, () -> 
                JdbcQueryableStorage.builder().connection(conn).queries(null).build());
        assertEquals("Queries are required", e1.getMessage());

        Exception e2 = assertThrows(InvalidParameterException.class, () -> 
                JdbcQueryableStorage.builder().connection(conn).queries(new Properties()).build());
        assertEquals("Queries are required", e2.getMessage());
    }

    @Test
    void shouldFailWhenQueryNameIsNullOrEmpty() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties queries = new Properties();
        queries.put("read", "select 1 from test");
        queries.put("write", "insert into test values (?)");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Exception e1 = assertThrows(InvalidParameterException.class, () -> storage.read(null));
            assertEquals("Invalid query name", e1.getMessage());

            Exception e2 = assertThrows(InvalidParameterException.class, () -> storage.read(""));
            assertEquals("Invalid query name", e2.getMessage());

            Exception e3 = assertThrows(InvalidParameterException.class, () -> storage.write(null, List.of("foo", "bar")));
            assertEquals("Invalid query name", e3.getMessage());

            Exception e4 = assertThrows(InvalidParameterException.class, () -> storage.write("", List.of("foo", "bar")));
            assertEquals("Invalid query name", e4.getMessage());
        }
    }

    @Test
    void shouldFailWhenQueryNotFound() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties queries = new Properties();
        queries.put("read", "select 1 from test");
        queries.put("write", "insert into test values (?)");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Exception e1 = assertThrows(QueryNotFoundException.class, () -> storage.read("foo"));
            assertEquals("Query foo not found", e1.getMessage());

            Exception e2 = assertThrows(QueryNotFoundException.class, () -> storage.write("foo", List.of("foo", "bar")));
            assertEquals("Query foo not found", e2.getMessage());
        }
    }

    @Test
    void shouldFailWhenStringParameterTooLong() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties queries = new Properties();
        queries.put("write", "insert into test values (?)");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            String longString = "a".repeat(101);
            Exception e = assertThrows(InvalidParameterException.class, () -> storage.write("write", List.of(longString)));
            assertEquals("Query parameter must be at most 100 characters", e.getMessage());
        }
    }

    @Test
    void shouldReadSingleRow() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(anyInt())).thenReturn("test_value");

        Properties queries = new Properties();
        queries.put("read", "select value from test");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Optional<List<Object>> result = storage.readSingle("read");

            assertTrue(result.isPresent());
            assertEquals("test_value", result.get().get(0));
        }
    }

    @Test
    void shouldReadSingleValue() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(anyInt())).thenReturn("single_value");

        Properties queries = new Properties();
        queries.put("read", "select value from test");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Optional<String> result = storage.readSingleValue("read");

            assertTrue(result.isPresent());
            assertEquals("single_value", result.get());
        }
    }

    @Test
    void shouldReadColumnValues() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(anyInt())).thenReturn("col1").thenReturn("col2");

        Properties queries = new Properties();
        queries.put("read", "select value from test");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Stream<String> values = storage.readColumnValues("read");
            List<String> result = values.toList();

            assertEquals(2, result.size());
            assertEquals("col1", result.get(0));
            assertEquals("col2", result.get(1));
        }
    }

    @Test
    void shouldReadAsStream() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        when(prepStmt.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSet.getObject(anyInt())).thenReturn("value1").thenReturn("value2");

        Properties queries = new Properties();
        queries.put("read", "select value from test");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .build()) {
            Stream<List<Object>> stream = storage.readAsStream("read");
            List<String> values = stream
                    .map(row -> (String) row.get(0))
                    .toList();

            assertEquals(2, values.size());
            assertEquals("value1", values.get(0));
            assertEquals("value2", values.get(1));
        }
    }

    @Test
    void shouldUseCustomConfigForStringLength() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties queries = new Properties();
        queries.put("write", "insert into test values (?)");

        StorageConfig config = StorageConfig.builder()
                .maxStringParamLength(50)
                .build();

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(queries)
                .config(config)
                .build()) {
            String longString = "a".repeat(51);
            Exception e = assertThrows(InvalidParameterException.class, () ->
                    storage.write("write", List.of(longString)));
            assertEquals("Query parameter must be at most 50 characters", e.getMessage());
        }
    }

    @Test
    void shouldHandleValidTypeCasts() {
        List<Object> row = List.of("string_value", 42, true);

        // These should work fine with correct types
        assertDoesNotThrow(() -> {
            String str = (String) row.get(0);
            assertEquals("string_value", str);
        });

        assertDoesNotThrow(() -> {
            Integer num = (Integer) row.get(1);
            assertEquals(Integer.valueOf(42), num);
        });

        assertDoesNotThrow(() -> {
            Boolean bool = (Boolean) row.get(2);
            assertEquals(Boolean.TRUE, bool);
        });
    }

    @Test
    void shouldFailGetValueWithInvalidIndex() {
        List<Object> row = List.of("value");

        assertThrows(IndexOutOfBoundsException.class,
                () -> row.get(1));
    }
}
