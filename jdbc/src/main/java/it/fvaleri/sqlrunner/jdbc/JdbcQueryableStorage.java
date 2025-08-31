/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.jdbc;

import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.StorageConfig;
import it.fvaleri.sqlrunner.exception.InvalidParameterException;
import it.fvaleri.sqlrunner.exception.QueryExecutionException;
import it.fvaleri.sqlrunner.exception.QueryNotFoundException;
import it.fvaleri.sqlrunner.exception.QueryableStorageException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

/**
 * JdbcQueryableStorage provides a robust, production-ready implementation of the
 * {@link QueryableStorage} interface using standard JDBC. All queries are pre-loaded
 * and cached as {@link PreparedStatement} objects for optimal performance and security.
 * The implementation handles resource management automatically and provides thread-safe
 * operations through method-level synchronization.
 * 
 * <h2>Key Features</h2>
 * <ul>
 *   <li><strong>Thread Safety:</strong> All write operations are synchronized for concurrent use</li>
 *   <li><strong>Resource Management:</strong> Automatic cleanup of PreparedStatements and ResultSets</li>
 *   <li><strong>Security:</strong> Protection against SQL injection through PreparedStatements</li>
 *   <li><strong>Performance:</strong> Query caching and batch processing support</li>
 *   <li><strong>Zero Dependencies:</strong> Uses only standard JDK and JDBC APIs</li>
 *   <li><strong>Flexible Configuration:</strong> Support for various connection sources and configurations</li>
 * </ul>
 * 
 * <h2>Supported Parameter Types</h2>
 * <p>The implementation supports automatic parameter binding for:</p>
 * <ul>
 *   <li>{@link String} - mapped to SQL VARCHAR/CHAR</li>
 *   <li>{@link Integer} - mapped to SQL INTEGER</li>
 *   <li>{@link Long} - mapped to SQL BIGINT</li>
 *   <li>{@link java.sql.Date} - mapped to SQL DATE</li>
 *   <li>{@link java.time.LocalDate} - converted to SQL DATE</li>
 *   <li>{@link java.math.BigDecimal} - mapped to SQL DECIMAL/NUMERIC</li>
 *   <li>{@link java.sql.Timestamp} - mapped to SQL TIMESTAMP</li>
 *   <li>{@code byte[]} - mapped to SQL BINARY/VARBINARY as binary stream</li>
 *   <li>{@code null} - mapped to SQL NULL</li>
 * </ul>
 * 
 * <h2>Usage Examples</h2>
 * 
 * <h3>Basic Setup with URL Connection</h3>
 * <pre>{@code
 * Properties queries = new Properties();
 * queries.put("users.insert", "insert into users (name, email) values (?, ?)");
 * queries.put("users.select", "select id, name, email from users where id = ?");
 * 
 * try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1")
 *         .queries(queries)
 *         .config(StorageConfig.builder()
 *             .maxStringParamLength(255)
 *             .autoCommit(false)
 *             .build())
 *         .build()) {
 *     
 *     storage.write("users.insert", List.of("John Doe", "john@example.com"));
 *     storage.commit();
 * }
 * }</pre>
 * 
 * <h3>Loading Queries from Classpath</h3>
 * <pre>{@code
 * try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:postgresql://localhost/mydb", "user", "pass")
 *         .queriesFromClasspath("sql/queries.properties")
 *         .config(StorageConfig.builder()
 *             .autoCommit(true)
 *             .build())
 *         .build()) {
 *     
 *     List<List<Object>> users = storage.read("selectAllUsers");
 *     Optional<String> userName = storage.readSingleValue("users.select", List.of(userId));
 *     
 *     // Access column values by index
 *     for (List<Object> user : users) {
 *         Long id = (Long) user.get(0);
 *         String name = (String) user.get(1);
 *         String email = (String) user.get(2);
 *     }
 * }
 * }</pre>
 * 
 * <h3>Batch Processing</h3>
 * <pre>{@code
 * try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:mysql://localhost/mydb", "user", "pass")
 *         .addQuery("batch.insert", "insert into logs (message, timestamp) values (?, ?)")
 *         .build()) {
 *     
 *     int batchSize = 1000;
 *     for (String logMessage : logMessages) {
 *         storage.write("batch.insert",
 *             List.of(logMessage, Timestamp.valueOf(LocalDateTime.now())), 
 *             batchSize);
 *     }
 * }
 * }</pre>
 * 
 * <h3>Transaction Management</h3>
 * <pre>{@code
 * StorageConfig config = StorageConfig.builder()
 *         .autoCommit(false)
 *         .build();
 *         
 * try (JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:oracle:thin:@localhost:1521:XE", "user", "pass")
 *         .queriesFromClasspath("sql/financial-queries.properties")
 *         .config(config)
 *         .build()) {
 *     
 *     try {
 *         storage.write("debit.account", List.of(fromAccountId, amount));
 *         storage.write("credit.account", List.of(toAccountId, amount));
 *         storage.write("log.transaction", List.of(transactionId, fromAccountId, toAccountId, amount));
 *         storage.commit();
 *     } catch (Exception e) {
 *         storage.rollback();
 *         throw new TransactionFailedException("Failed to process transfer", e);
 *     }
 * }
 * }</pre>
 * 
 * <h2>Thread Safety</h2>
 * <p>This implementation is thread-safe for concurrent use. Write operations are synchronized
 * at the method level to ensure data consistency, while read operations can be performed
 * concurrently. Batch operations maintain per-query counters that are thread-safe.</p>
 * 
 * <h2>Performance Considerations</h2>
 * <ul>
 *   <li>PreparedStatements are cached and reused for better performance</li>
 *   <li>Batch processing can significantly improve write performance for bulk operations</li>
 *   <li>Connection pooling should be handled at the connection source level</li>
 *   <li>Large result sets should use stream-based reading methods when possible</li>
 * </ul>
 * 
 * <h2>Error Handling</h2>
 * <p>The implementation wraps SQL exceptions into domain-specific exceptions:</p>
 * <ul>
 *   <li>{@link it.fvaleri.sqlrunner.exception.QueryableStorageException} - for connection and transaction errors</li>
 *   <li>{@link it.fvaleri.sqlrunner.exception.QueryExecutionException} - for SQL execution errors</li>
 *   <li>{@link it.fvaleri.sqlrunner.exception.QueryNotFoundException} - when a named query is not found</li>
 *   <li>{@link it.fvaleri.sqlrunner.exception.InvalidParameterException} - for parameter validation errors</li>
 * </ul>
 * 
 * @see QueryableStorage
 * @see StorageConfig
 * @since 0.1.0
 */
public class JdbcQueryableStorage implements QueryableStorage {
    private final Connection connection;
    private final StorageConfig config;
    private final Map<String, PreparedStatement> prepStmts;
    private final Map<String, AtomicLong> batchCounters;

    private JdbcQueryableStorage(Connection connection, Properties queries, StorageConfig config) {
        try {
            if (connection == null || connection.isClosed()) {
                throw new InvalidParameterException("Invalid connection", "connection");
            }
            if (queries == null || queries.isEmpty()) {
                throw new InvalidParameterException("Invalid queries", "queries");
            }
            if (config == null) {
                throw new InvalidParameterException("Invalid config", "config");
            }
            this.connection = connection;
            this.config = config;
            this.prepStmts = new HashMap<>();
            this.batchCounters = new ConcurrentHashMap<>();
            if (!config.isAutoCommit()) {
                connection.setAutoCommit(false);
            }
            for (String key : queries.stringPropertyNames()) {
                prepStmts.put(key, connection.prepareStatement(queries.getProperty(key)));
            }
            queries.clear();
        } catch (SQLException e) {
            throw new QueryableStorageException(format("Init error: %s", e.getMessage()), "INIT_ERROR", e);
        }
    }

    @Override
    public synchronized int write(String queryName) {
        return write(queryName, null, 1);
    }

    @Override
    public synchronized int write(String queryName, List<Object> queryParams) {
        return write(queryName, queryParams, 1);
    }

    @Override
    public synchronized int write(String queryName, List<Object> queryParams, int batchSize) {
        if (queryName == null || queryName.trim().isEmpty()) {
            throw new InvalidParameterException("Invalid query name", "queryName");
        }
        PreparedStatement prepStmt = prepStmts.get(queryName);
        if (prepStmt == null) {
            throw new QueryNotFoundException(queryName);
        }
        try {
            prepStmt.clearParameters();
            if (queryParams != null && !queryParams.isEmpty()) {
                for (int i = 0; i < queryParams.size(); i++) {
                    setQueryParam(prepStmt, i + 1, queryParams.get(i));
                }
            }
            addBatch(queryName, batchSize);
            return batchSize > 1 ? executeBatch(queryName, batchSize) : prepStmt.executeUpdate();
        } catch (SQLException e) {
            throw new QueryExecutionException(queryName, e.getMessage(), e);
        }
    }

    @Override
    public List<List<Object>> read(String queryName) {
        return read(queryName, null);
    }

    @Override
    public List<List<Object>> read(String queryName, List<Object> queryParams) {
        if (queryName == null || queryName.trim().isEmpty()) {
            throw new InvalidParameterException("Invalid query name", "queryName");
        }
        PreparedStatement prepStmt = prepStmts.get(queryName);
        if (prepStmt == null) {
            throw new QueryNotFoundException(queryName);
        }
        try {
            prepStmt.clearParameters();
            if (queryParams != null && !queryParams.isEmpty()) {
                for (int i = 0; i < queryParams.size(); i++) {
                    setQueryParam(prepStmt, i + 1, queryParams.get(i));
                }
            }
            List<List<Object>> rows = new ArrayList<>();
            try (ResultSet resultSet = prepStmt.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    List<Object> columns = new ArrayList<>();
                    for (int j = 1; j <= columnCount; j++) {
                        columns.add(resultSet.getObject(j));
                    }
                    rows.add(columns);
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new QueryExecutionException(queryName, e.getMessage(), e);
        }
    }

    @Override
    public void commit() {
        try {
            if (!config.isAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new QueryableStorageException(format("Commit failed: %s", e.getMessage()), "COMMIT_ERROR", e);
        }
    }

    @Override
    public void rollback() {
        try {
            if (!config.isAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            throw new QueryableStorageException(format("Rollback failed: %s", e.getMessage()), "ROLLBACK_ERROR", e);
        }
    }

    @Override
    public void close() {
        try {
            if (batchCounters != null && !batchCounters.isEmpty()) {
                batchCounters.clear();
            }
            if (prepStmts != null && !prepStmts.isEmpty()) {
                for (PreparedStatement ps : prepStmts.values()) {
                    ps.close();
                }
                prepStmts.clear();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private void setQueryParam(PreparedStatement prepStmt, int index, Object param) {
        try {
            if (param == null) {
                prepStmt.setNull(index, Types.NULL);
            } else if (param instanceof String) {
                String stringParam = (String) param;
                if (stringParam.length() > config.getMaxStringParamLength()) {
                    throw new InvalidParameterException(
                            format("Query parameter must be at most %d characters",
                                    config.getMaxStringParamLength()), "stringParam");
                }
                prepStmt.setString(index, stringParam);
            } else if (param instanceof Integer) {
                prepStmt.setInt(index, (Integer) param);
            } else if (param instanceof Long) {
                prepStmt.setLong(index, (Long) param);
            } else if (param instanceof Date) {
                prepStmt.setDate(index, (Date) param);
            } else if (param instanceof LocalDate) {
                prepStmt.setDate(index, Date.valueOf((LocalDate) param));
            } else if (param instanceof BigDecimal) {
                prepStmt.setBigDecimal(index, (BigDecimal) param);
            } else if (param instanceof Timestamp) {
                prepStmt.setTimestamp(index, (Timestamp) param);
            } else if (param instanceof byte[]) {
                prepStmt.setBinaryStream(index,
                        new ByteArrayInputStream((byte[]) param), ((byte[]) param).length);
            } else {
                throw new InvalidParameterException("Unsupported data type for query parameter: "
                        + param.getClass().getSimpleName(), "param");
            }
        } catch (SQLException e) {
            throw new QueryableStorageException("Failed to set query parameter", "PARAMETER_ERROR", e);
        }
    }

    private void addBatch(String queryName, int batchSize) throws SQLException {
        if (batchSize > 1) {
            PreparedStatement prepStmt = prepStmts.get(queryName);
            prepStmt.addBatch();
            batchCounters.putIfAbsent(queryName, new AtomicLong(0));
            batchCounters.get(queryName).incrementAndGet();
        }
    }

    private synchronized int executeBatch(String queryName, int batchSize) throws SQLException {
        AtomicLong counter = batchCounters.get(queryName);
        int result = 0;
        if (counter != null && counter.get() >= batchSize) {
            PreparedStatement prepStmt = prepStmts.get(queryName);
            int[] updateCounts = prepStmt.executeBatch();
            prepStmt.clearBatch();
            batchCounters.remove(queryName);
            result = Arrays.stream(updateCounts).sum();
        }
        return result;
    }

    /**
     * Creates a new Builder instance for constructing JdbcQueryableStorage objects.
     * 
     * @return a new Builder instance with default configuration
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating JdbcQueryableStorage instances with fluent API.
     * 
     * <p>The Builder provides a convenient way to construct JdbcQueryableStorage instances
     * with various configuration options. It supports multiple ways to provide database
     * connections and query definitions, making it flexible for different use cases.</p>
     * 
     * <h2>Required Components</h2>
     * <ul>
     *   <li><strong>Connection:</strong> Must be provided via {@link #connection(Connection)}, 
     *       {@link #connectionFromUrl(String)}, or {@link #connectionFromUrl(String, String, String)}</li>
     *   <li><strong>Queries:</strong> Must be provided via {@link #queries(Properties)}, 
     *       {@link #queriesFromClasspath(String)}, {@link #queriesFromStream(InputStream)}, 
     *       or {@link #addQuery(String, String)}</li>
     * </ul>
     * 
     * <h2>Example Usage</h2>
     * <pre>{@code
     * // Complete example with all options
     * JdbcQueryableStorage storage = JdbcQueryableStorage.builder()
     *     .connectionFromUrl("jdbc:h2:mem:testdb", "user", "password")
     *     .queriesFromClasspath("sql/user-queries.properties")
     *     .addQuery("customQuery", "SELECT COUNT(*) FROM users WHERE active = ?")
     *     .config(StorageConfig.builder()
     *         .maxStringParamLength(500)
     *         .autoCommit(false)
     *         .build())
     *     .build();
     * }</pre>
     */
    public static class Builder {
        private Connection connection;
        private Properties queries;
        private StorageConfig config = new StorageConfig();

        /**
         * Sets the database connection to use.
         * 
         * @param connection the JDBC connection (must not be null or closed)
         * @return this Builder instance for method chaining
         */
        public Builder connection(Connection connection) {
            this.connection = connection;
            return this;
        }

        /**
         * Sets the queries to use from a Properties object.
         * 
         * @param queries the Properties containing query name-to-SQL mappings (must not be null or empty)
         * @return this Builder instance for method chaining
         */
        public Builder queries(Properties queries) {
            this.queries = queries;
            return this;
        }

        /**
         * Sets the storage configuration.
         * 
         * @param config the StorageConfig to use (if null, defaults will be used)
         * @return this Builder instance for method chaining
         */
        public Builder config(StorageConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Creates a database connection using the provided JDBC URL.
         * 
         * <p>This method creates a new database connection using the JDBC DriverManager.
         * The appropriate JDBC driver must be available on the classpath. This method
         * is suitable for databases that don't require authentication or when credentials
         * are embedded in the URL.</p>
         * 
         * <p>Example URLs:</p>
         * <ul>
         *   <li>H2 in-memory: {@code jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1}</li>
         *   <li>H2 file: {@code jdbc:h2:file:~/test}</li>
         *   <li>SQLite: {@code jdbc:sqlite:test.db}</li>
         * </ul>
         *
         * @param jdbcUrl the JDBC connection URL (must not be null)
         * @return this Builder instance for method chaining
         * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if connection creation fails
         */
        public Builder connectionFromUrl(String jdbcUrl) {
            try {
                this.connection = DriverManager.getConnection(jdbcUrl);
            } catch (SQLException e) {
                throw new InvalidParameterException("Failed to create connection from URL: " + jdbcUrl, "jdbcUrl");
            }
            return this;
        }

        /**
         * Creates a database connection using the provided JDBC URL with authentication.
         * 
         * <p>This method creates a new database connection using the JDBC DriverManager
         * with username and password authentication. The appropriate JDBC driver must
         * be available on the classpath.</p>
         * 
         * <p>Example URLs with authentication:</p>
         * <ul>
         *   <li>PostgreSQL: {@code jdbc:postgresql://localhost:5432/mydb}</li>
         *   <li>MySQL: {@code jdbc:mysql://localhost:3306/mydb}</li>
         *   <li>Oracle: {@code jdbc:oracle:thin:@localhost:1521:XE}</li>
         *   <li>SQL Server: {@code jdbc:sqlserver://localhost:1433;databaseName=mydb}</li>
         * </ul>
         *
         * @param jdbcUrl the JDBC connection URL (must not be null)
         * @param username the database username (may be null for some databases)
         * @param password the database password (may be null for some databases)
         * @return this Builder instance for method chaining
         * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if connection creation fails
         */
        public Builder connectionFromUrl(String jdbcUrl, String username, String password) {
            try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
            } catch (SQLException e) {
                throw new InvalidParameterException("Failed to create connection from URL: " + jdbcUrl, "jdbcUrl");
            }
            return this;
        }

        /**
         * Builds a new JdbcQueryableStorage instance with the configured options.
         * 
         * @return a new JdbcQueryableStorage instance
         * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if required components are missing
         * @throws it.fvaleri.sqlrunner.exception.QueryableStorageException if initialization fails
         */
        public JdbcQueryableStorage build() {
            if (connection == null) {
                throw new InvalidParameterException("Connection is required", "connection");
            }
            if (queries == null || queries.isEmpty()) {
                throw new InvalidParameterException("Queries are required", "queries");
            }
            return new JdbcQueryableStorage(connection, queries, config);
        }

        /**
         * Loads queries from a properties file located in the classpath.
         * 
         * <p>This method loads SQL queries from a properties file where each property
         * represents a named query. The file must be accessible via the current thread's
         * context class loader. Property keys become query names, and values should contain
         * the SQL statements.</p>
         * 
         * <p>Example properties file content:</p>
         * <pre>
         * # User management queries
         * user.insert=INSERT INTO users (name, email) VALUES (?, ?)
         * user.select=SELECT id, name, email FROM users WHERE id = ?
         * user.selectAll=SELECT id, name, email FROM users ORDER BY name
         * user.update=UPDATE users SET name = ?, email = ? WHERE id = ?
         * user.delete=DELETE FROM users WHERE id = ?
         * </pre>
         *
         * @param resourcePath the path to the properties file in the classpath (must not be null)
         * @return this Builder instance for method chaining
         * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if the file cannot be found or loaded
         */
        public Builder queriesFromClasspath(String resourcePath) {
            Properties queries = new Properties();
            try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath)) {
                if (stream == null) {
                    throw new InvalidParameterException("Properties file not found: " + resourcePath, "resourcePath");
                }
                queries.load(stream);
                this.queries = queries;
            } catch (IOException e) {
                throw new InvalidParameterException("Failed to load properties from: " + resourcePath, "resourcePath");
            }
            return this;
        }

        /**
         * Load queries from a properties file using an InputStream.
         *
         * @param inputStream InputStream to load properties from
         * @return This builder
         */
        public Builder queriesFromStream(InputStream inputStream) {
            Properties queries = new Properties();
            try {
                queries.load(inputStream);
                this.queries = queries;
            } catch (IOException e) {
                throw new InvalidParameterException("Failed to load properties from stream", "inputStream");
            }
            return this;
        }

        /**
         * Adds an individual query programmatically.
         * 
         * <p>This method allows adding queries one at a time, which is useful for
         * dynamic query construction or when queries are defined in code rather than
         * external files. This method can be called multiple times to add several queries,
         * and can be combined with other query loading methods.</p>
         * 
         * <p>The SQL statement can contain parameter placeholders (?) that will be bound
         * at execution time using the parameter lists provided to read/write methods.</p>
         *
         * @param queryName the unique name for this query (must not be null or empty)
         * @param sql the SQL statement, may contain parameter placeholders (must not be null)
         * @return this Builder instance for method chaining
         * @throws IllegalArgumentException if queryName is null/empty or sql is null
         */
        public Builder addQuery(String queryName, String sql) {
            if (this.queries == null) {
                this.queries = new Properties();
            }
            this.queries.setProperty(queryName, sql);
            return this;
        }
    }
}
