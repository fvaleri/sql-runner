/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.jdbc;

import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.StorageConfig;
import it.fvaleri.sqlrunner.exception.InvalidParameterException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcQueryableStorageBuilderTest {
    @Test
    void shouldLoadQueriesFromClasspath() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queriesFromClasspath("queries.properties")
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldLoadQueriesFromStream() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        String propertiesContent = "test.query=select * from test";
        ByteArrayInputStream stream = new ByteArrayInputStream(propertiesContent.getBytes());

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queriesFromStream(stream)
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldAddQueriesProgrammatically() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .addQuery("query1", "select 1")
                .addQuery("query2", "select 2")
                .addQuery("query3", "select 3")
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldCombinePropertiesAndProgrammaticQueries() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Properties initialQueries = new Properties();
        initialQueries.setProperty("initial.query", "select 'initial'");

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .queries(initialQueries)
                .addQuery("added.query", "select 'added'")
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldFailWithInvalidClasspathResource() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Exception e = assertThrows(InvalidParameterException.class, () ->
                JdbcQueryableStorage.builder()
                        .connection(conn)
                        .queriesFromClasspath("non-existent.properties")
                        .build());
        
        assertEquals("Properties file not found: non-existent.properties", e.getMessage());
    }

    @Test
    void shouldFailWithEmptyQueries() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);

        Exception e = assertThrows(InvalidParameterException.class, () ->
                JdbcQueryableStorage.builder()
                        .connection(conn)
                        .queries(new Properties())  // Empty properties
                        .build());
        
        assertEquals("Queries are required", e.getMessage());
    }

    @Test
    void shouldCombineConfigurationMethods() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement prepStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(prepStmt);
        StorageConfig config = StorageConfig.builder()
                .maxStringParamLength(500)
                .batchSize(50)
                .autoCommit(false)
                .build();

        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(conn)
                .addQuery("test.query", "select 1")
                .config(config)
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldCreateConnectionFromUrl() throws Exception {
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connectionFromUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1")
                .addQuery("test.query", "select 1")
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldCreateConnectionFromUrlWithCredentials() throws Exception {
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connectionFromUrl("jdbc:h2:mem:testdb2;DB_CLOSE_DELAY=-1", "sa", "")
                .addQuery("test.query", "select 1")
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldCreateH2InMemoryConnection() throws Exception {
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connectionFromUrl("jdbc:h2:mem:testdb3")
                .addQuery("test.query", "select 1")
                .build()) {
            
            assertNotNull(storage);
        }
    }

    @Test
    void shouldFailToCreateH2InMemoryConnectionWithInvalidInitScript() {
        Exception e = assertThrows(InvalidParameterException.class, () ->
                JdbcQueryableStorage.builder()
                        .connectionFromUrl("jdbc:h2:mem:testdb4;INIT=runscript from 'classpath:/non-existent.sql'")
                        .addQuery("test.query", "select 1")
                        .build());
        
        assertTrue(e.getMessage().contains("Failed to create connection from URL"));
    }

    @Test
    void shouldFailWithInvalidJdbcUrl() {
        Exception e = assertThrows(InvalidParameterException.class, () ->
                JdbcQueryableStorage.builder()
                        .connectionFromUrl("invalid-url")
                        .addQuery("test.query", "select 1")
                        .build());
        
        assertTrue(e.getMessage().contains("Failed to create connection from URL"));
    }
}
