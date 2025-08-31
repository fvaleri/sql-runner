/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.jdbc;

import it.fvaleri.sqlrunner.StorageConfig;
import it.fvaleri.sqlrunner.exception.QueryableStorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcQueryableStorageTransactionTest {
    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;
    private Properties queries;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = mock(Connection.class);
        mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
        when(mockConnection.isClosed()).thenReturn(false);
        
        queries = new Properties();
        queries.put("test.insert", "insert into test_table (name) values (?)");
        queries.put("test.select", "select name from test_table where id = ?");
    }

    @Test
    void shouldCommitWhenAutoCommitIsDisabled() throws SQLException {
        StorageConfig config = StorageConfig.builder().autoCommit(false).build();
        
        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.commit();
            verify(mockConnection).commit();
        }
    }

    @Test
    void shouldNotCommitWhenAutoCommitIsEnabled() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(true)
                .build();

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.commit();
            verify(mockConnection, never()).commit();
        }
    }

    @Test
    void shouldRollbackWhenAutoCommitIsDisabled() throws SQLException {
        StorageConfig config = StorageConfig.builder().autoCommit(false).build();
        
        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.rollback();
            verify(mockConnection).rollback();
        }
    }

    @Test
    void shouldNotRollbackWhenAutoCommitIsEnabled() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(true)
                .build();

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.rollback();
            verify(mockConnection, never()).rollback();
        }
    }

    @Test
    void shouldThrowExceptionWhenCommitFails() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        doThrow(new SQLException("Commit failed")).when(mockConnection).commit();

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            QueryableStorageException exception = assertThrows(QueryableStorageException.class, 
                    storage::commit);
            assertEquals("Commit failed: Commit failed", exception.getMessage());
            assertEquals("COMMIT_ERROR", exception.getErrorCode());
        }
    }

    @Test
    void shouldThrowExceptionWhenRollbackFails() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        doThrow(new SQLException("Rollback failed")).when(mockConnection).rollback();

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            QueryableStorageException exception = assertThrows(QueryableStorageException.class, 
                    storage::rollback);
            assertEquals("Rollback failed: Rollback failed", exception.getMessage());
            assertEquals("ROLLBACK_ERROR", exception.getErrorCode());
        }
    }

    @Test
    void shouldCommitSuccessfullyAfterWriteOperations() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.write("test.insert", List.of("test_value"));
            storage.commit();
            
            verify(mockConnection).commit();
            verify(mockPreparedStatement).executeUpdate();
        }
    }

    @Test
    void shouldRollbackAfterFailedOperations() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.write("test.insert", List.of("test_value"));
            storage.rollback();
            
            verify(mockConnection).rollback();
            verify(mockPreparedStatement).executeUpdate();
        }
    }

    @Test
    void shouldHandleMultipleCommitsInTransaction() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.write("test.insert", List.of("value1"));
            storage.commit();
            
            storage.write("test.insert", List.of("value2"));
            storage.commit();
            
            verify(mockConnection, times(2)).commit();
        }
    }

    @Test
    void shouldHandleCommitRollbackSequence() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            storage.write("test.insert", List.of("value1"));
            storage.commit();
            
            storage.write("test.insert", List.of("value2"));
            storage.rollback();
            
            storage.write("test.insert", List.of("value3"));
            storage.commit();
            
            verify(mockConnection, times(2)).commit();
            verify(mockConnection).rollback();
        }
    }

    @Test
    void shouldWorkWithBuilderAutoCommitFalse() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(false)
                .build();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            
            storage.write("test.insert", List.of("test_value"));
            storage.commit();
            
            verify(mockConnection).commit();
        }
    }

    @Test
    void shouldWorkWithBuilderAutoCommitTrue() throws SQLException {
        StorageConfig config = StorageConfig.builder()
                .autoCommit(true)
                .build();

        when(mockPreparedStatement.executeUpdate()).thenReturn(1);

        try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
                .connection(mockConnection)
                .queries(queries)
                .config(config)
                .build()) {
            
            storage.write("test.insert", List.of("test_value"));
            storage.commit();
            
            verify(mockConnection, never()).commit();
        }
    }
}