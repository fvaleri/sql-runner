/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * QueryableStorage provides a high-level abstraction for executing pre-defined SQL queries
 * by name, supporting both read and write operations with parameter binding, batch processing,
 * and transaction control. All queries must be pre-loaded and registered with the storage
 * implementation before use.
 * 
 * <h2>Key Features</h2>
 * <ul>
 *   <li><strong>Named Queries:</strong> Execute SQL queries by name with type-safe parameter binding</li>
 *   <li><strong>Batch Processing:</strong> Support for batch writes to improve performance</li>
 *   <li><strong>Transaction Control:</strong> Manual commit/rollback support when auto-commit is disabled</li>
 *   <li><strong>Flexible Reading:</strong> Multiple ways to read data - as lists, streams, single values, or typed columns</li>
 *   <li><strong>Thread Safety:</strong> Implementations may provide thread-safe operations</li>
 *   <li><strong>Resource Management:</strong> Implements AutoCloseable for proper resource cleanup</li>
 * </ul>
 * 
 * <h2>Supported Parameter Types</h2>
 * <p>Query parameters support the following Java types:</p>
 * <ul>
 *   <li>{@link String} - for text data</li>
 *   <li>{@link Integer}, {@link Long} - for numeric data</li>
 *   <li>{@link java.sql.Date}, {@link java.time.LocalDate} - for date values</li>
 *   <li>{@link java.math.BigDecimal} - for precise decimal values</li>
 *   <li>{@link java.sql.Timestamp} - for timestamp values</li>
 *   <li>{@code byte[]} - for binary data</li>
 *   <li>{@code null} - for NULL values</li>
 * </ul>
 * 
 * <h2>Usage Examples</h2>
 * 
 * <h3>Basic Read/Write Operations</h3>
 * <pre>{@code
 * // Create storage with pre-defined queries
 * Properties queries = new Properties();
 * queries.put("users.insert", "insert into users (name, email) values (?, ?)");
 * queries.put("users.select", "select id, name, email from users where id = ?");
 * queries.put("users.select.all", "select id, name, email from users");
 * 
 * try (QueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:h2:mem:testdb")
 *         .queries(queries)
 *         .build()) {
 *     
 *     // Write operations
 *     int rowsInserted = storage.write("users.insert", List.of("John Doe", "john@example.com"));
 *     
 *     // Read operations - each row is a List<Object>
 *     List<List<Object>> users = storage.read("users.select.all");
 *     Optional<List<Object>> user = storage.readSingle("users.select", List.of(1));
 *     
 *     // Access column values by index
 *     if (user.isPresent()) {
 *         Long id = (Long) user.get().get(0);
 *         String name = (String) user.get().get(1);
 *         String email = (String) user.get().get(2);
 *     }
 *     
 *     // Single value reading
 *     Optional<String> userName = storage.readSingleValue("users.select", List.of(1));
 * }
 * }</pre>
 * 
 * <h3>Batch Processing</h3>
 * <pre>{@code
 * try (QueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:h2:mem:testdb")
 *         .queries(queries)
 *         .build()) {
 *     
 *     // Batch writes - executes when batch size is reached
 *     int batchSize = 100;
 *     storage.write("users.insert", List.of("User1", "user1@example.com"), batchSize);
 *     storage.write("users.insert", List.of("User2", "user2@example.com"), batchSize);
 *     // ... continue until batch size is reached
 * }
 * }</pre>
 * 
 * <h3>Transaction Management</h3>
 * <pre>{@code
 * StorageConfig config = StorageConfig.builder()
 *         .autoCommit(false)
 *         .build();
 *         
 * try (QueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:h2:mem:testdb")
 *         .queries(queries)
 *         .config(config)
 *         .build()) {
 *     
 *     try {
 *         storage.write("users.insert", List.of("John", "john@example.com"));
 *         storage.write("orders.insert", List.of(1, "Product A"));
 *         storage.commit(); // Commit both operations
 *     } catch (Exception e) {
 *         storage.rollback(); // Rollback on error
 *         throw e;
 *     }
 * }
 * }</pre>
 * 
 * <h3>Stream Processing</h3>
 * <pre>{@code
 * try (QueryableStorage storage = JdbcQueryableStorage.builder()
 *         .connectionFromUrl("jdbc:h2:mem:testdb")
 *         .queries(queries)
 *         .build()) {
 *     
 *     // Process large result sets as streams
 *     List<String> userNames = storage.readColumnValues("users.select.all")
 *             .filter(name -> name.startsWith("John"))
 *             .limit(10)
 *             .toList();
 *             
 *     // Custom row processing
 *     storage.readAsStream("users.select.all")
 *             .map(row -> new User((Long) row.get(0), (String) row.get(1), (String) row.get(2)))
 *             .forEach(user -> processUser(user));
 * }
 * }</pre>
 *
 * @see StorageConfig
 * @since 0.1.0
 */
public interface QueryableStorage extends AutoCloseable {
    /**
     * Executes a write query without parameters.
     * 
     * <p>This method executes a pre-registered SQL statement identified by the query name.
     * The query should not contain any parameter placeholders (?).</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @return the number of rows affected by the write operation
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null or empty
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    int write(String queryName);

    /**
     * Executes a write query with parameters.
     * 
     * <p>This method executes a pre-registered SQL statement with parameter binding.
     * The query parameters order must match the parameter placeholder order in the SQL statement.
     * Parameters are bound using their natural JDBC types based on the Java type.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @return the number of rows affected by the write operation
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    int write(String queryName, List<Object> queryParams);

    /**
     * Executes a write query with parameters and batch support.
     * 
     * <p>This method supports batch processing for improved performance when executing
     * multiple write operations. When batchSize is greater than 1, the write operation
     * is accumulated and executed only when the specified batch size is reached for
     * that particular query name.</p>
     * 
     * <p>The query parameters order must match the parameter placeholder order in the SQL statement.
     * Each call with the same queryName contributes to the same batch counter.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @param batchSize the number of operations to accumulate before execution (must be positive)
     * @return the number of rows affected when the batch is executed, or 0 if batch is not yet full
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    int write(String queryName, List<Object> queryParams, int batchSize);

    /**
     * Executes a read query without parameters.
     * 
     * <p>This method executes a pre-registered SELECT statement and returns all matching rows.
     * Column types are automatically inferred from database metadata. The query should not
     * contain any parameter placeholders (?).</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @return a list of row data, where each row is a {@link List} of column values (empty list if no results)
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null or empty
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    List<List<Object>> read(String queryName);

    /**
     * Executes a read query with parameters.
     * 
     * <p>This method executes a pre-registered SELECT statement with parameter binding
     * and returns all matching rows. Column types are automatically inferred from database
     * metadata. The query parameters order must match the parameter placeholder order in the SQL statement.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @return a list of row data, where each row is a {@link List} of column values (empty list if no results)
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    List<List<Object>> read(String queryName, List<Object> queryParams);

    /**
     * Commits the current transaction.
     * 
     * <p>This method commits all pending write operations in the current transaction.
     * Only effective when autoCommit is disabled in the storage configuration.
     * When autoCommit is enabled, this method has no effect.</p>
     *
     * @throws it.fvaleri.sqlrunner.exception.QueryableStorageException if the commit operation fails
     */
    void commit();

    /**
     * Rolls back the current transaction.
     * 
     * <p>This method rolls back all pending write operations in the current transaction,
     * undoing any changes made since the last commit. Only effective when autoCommit is
     * disabled in the storage configuration. When autoCommit is enabled, this method has no effect.</p>
     *
     * @throws it.fvaleri.sqlrunner.exception.QueryableStorageException if the rollback operation fails
     */
    void rollback();

    /**
     * Executes a read query returning a single result.
     * 
     * <p>This convenience method executes a read query and returns only the first row
     * if any results are found. Column types are automatically inferred from database metadata.
     * This is useful for queries that are expected to return at most one row.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @return an {@link Optional} containing the first row data as a {@link List} if results exist, empty otherwise
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null or empty
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    default Optional<List<Object>> readSingle(String queryName) {
        List<List<Object>> rows = read(queryName);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /**
     * Executes a read query returning a single result with parameters.
     * 
     * <p>This convenience method executes a read query with parameter binding and returns
     * only the first row if any results are found. Column types are automatically inferred
     * from database metadata. This is useful for queries that are expected to return at most one row.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @return an {@link Optional} containing the first row data as a {@link List} if results exist, empty otherwise
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    default Optional<List<Object>> readSingle(String queryName, List<Object> queryParams) {
        List<List<Object>> rows = read(queryName, queryParams);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /**
     * Reads a single column value from the first row.
     * 
     * <p>This convenience method executes a read query and extracts the value from the first
     * column of the first row. This is particularly useful for aggregate queries (COUNT, SUM, etc.)
     * or queries that select a single column value.</p>
     * 
     * <p><strong>Note:</strong> The caller is responsible for ensuring the returned type matches
     * the actual column type. A {@link ClassCastException} will be thrown if the types don't match.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param <T> the expected type of the column value
     * @return an {@link Optional} containing the typed value if results exist, empty otherwise
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null or empty
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     * @throws ClassCastException if the column value cannot be cast to type T
     */
    @SuppressWarnings("unchecked")
    default <T> Optional<T> readSingleValue(String queryName) {
        return readSingle(queryName)
                .map(row -> (T) row.get(0));
    }

    /**
     * Reads a single column value from the first row with parameters.
     * 
     * <p>This convenience method executes a read query with parameter binding and extracts
     * the value from the first column of the first row. This is particularly useful for
     * aggregate queries (COUNT, SUM, etc.) or queries that select a single column value.</p>
     * 
     * <p><strong>Note:</strong> The caller is responsible for ensuring the returned type matches
     * the actual column type. A {@link ClassCastException} will be thrown if the types don't match.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @param <T> the expected type of the column value
     * @return an {@link Optional} containing the typed value if results exist, empty otherwise
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     * @throws ClassCastException if the column value cannot be cast to type T
     */
    @SuppressWarnings("unchecked")
    default <T> Optional<T> readSingleValue(String queryName, List<Object> queryParams) {
        return readSingle(queryName, queryParams)
                .map(row -> (T) row.get(0));
    }

    /**
     * Reads all values from a single column as a Stream.
     * 
     * <p>This convenience method executes a read query and extracts values from the first
     * column of all rows, returning them as a Stream. This is useful for processing large
     * result sets efficiently or when you only need values from a single column.</p>
     * 
     * <p><strong>Note:</strong> The caller is responsible for ensuring the returned type matches
     * the actual column type. A {@link ClassCastException} will be thrown if the types don't match.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param <T> the expected type of the column values
     * @return a {@link Stream} of typed values from the first column
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null or empty
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     * @throws ClassCastException if any column value cannot be cast to type T
     */
    @SuppressWarnings("unchecked")
    default <T> Stream<T> readColumnValues(String queryName) {
        return readAsStream(queryName)
                .map(row -> (T) row.get(0));
    }

    /**
     * Reads all values from a single column as a Stream with parameters.
     * 
     * <p>This convenience method executes a read query with parameter binding and extracts
     * values from the first column of all rows, returning them as a Stream. This is useful
     * for processing large result sets efficiently or when you only need values from a single column.</p>
     * 
     * <p><strong>Note:</strong> The caller is responsible for ensuring the returned type matches
     * the actual column type. A {@link ClassCastException} will be thrown if the types don't match.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @param <T> the expected type of the column values
     * @return a {@link Stream} of typed values from the first column
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     * @throws ClassCastException if any column value cannot be cast to type T
     */
    @SuppressWarnings("unchecked")
    default <T> Stream<T> readColumnValues(String queryName, List<Object> queryParams) {
        return readAsStream(queryName, queryParams)
                .map(row -> (T) row.get(0));
    }

    /**
     * Executes a read query and returns results as a Stream.
     * 
     * <p>This convenience method executes a read query and returns all results as a Stream
     * for efficient processing of large result sets. Column types are automatically inferred
     * from database metadata. This is particularly useful when you need to process results
     * with filtering, mapping, or other stream operations.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @return a {@link Stream} of row data, where each row is a {@link List} of column values
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null or empty
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    default Stream<List<Object>> readAsStream(String queryName) {
        return read(queryName).stream();
    }

    /**
     * Executes a read query with parameters and returns results as a Stream.
     * 
     * <p>This convenience method executes a read query with parameter binding and returns
     * all results as a Stream for efficient processing of large result sets. Column types
     * are automatically inferred from database metadata. This is particularly useful when
     * you need to process results with filtering, mapping, or other stream operations.</p>
     *
     * @param queryName the name of the pre-registered query to execute (must not be null or empty)
     * @param queryParams the list of parameters to bind to the query (may be null or empty)
     * @return a {@link Stream} of row data, where each row is a {@link List} of column values
     * @throws it.fvaleri.sqlrunner.exception.InvalidParameterException if queryName is null/empty or parameter validation fails
     * @throws it.fvaleri.sqlrunner.exception.QueryNotFoundException if no query is registered with the given name
     * @throws it.fvaleri.sqlrunner.exception.QueryExecutionException if the query execution fails
     */
    default Stream<List<Object>> readAsStream(String queryName, List<Object> queryParams) {
        return read(queryName, queryParams).stream();
    }

}
