/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner;

/**
 * Configuration for QueryableStorage implementations.
 * 
 * <p>This class provides configuration options for QueryableStorage implementations,
 * including string parameter length limits, batch processing settings, and transaction control.</p>
 * 
 * <p>Example usage:</p>
 * <pre>{@code
 * StorageConfig config = StorageConfig.builder()
 *     .maxStringParamLength(500)
 *     .batchSize(100)
 *     .autoCommit(false)
 *     .build();
 * }</pre>
 * 
 * @since 0.1.0
 */
public class StorageConfig {
    /** Default maximum length for string parameters. */
    public static final int DEFAULT_MAX_STRING_PARAM_LENGTH = 100;
    
    /** Default batch size for batch operations. */
    public static final int DEFAULT_BATCH_SIZE = 1;
    
    /** Default auto-commit setting. */
    public static final boolean DEFAULT_AUTO_COMMIT = true;

    private final int maxStringParamLength;
    private final int batchSize;
    private final boolean autoCommit;

    /**
     * Creates a new StorageConfig with default values.
     * 
     * <p>Default values are:</p>
     * <ul>
     *   <li>maxStringParamLength: 100 characters</li>
     *   <li>batchSize: 1 (no batching)</li>
     *   <li>autoCommit: true</li>
     * </ul>
     */
    public StorageConfig() {
        this(DEFAULT_MAX_STRING_PARAM_LENGTH, DEFAULT_BATCH_SIZE, DEFAULT_AUTO_COMMIT);
    }

    /**
     * Creates a new StorageConfig with the specified parameters.
     * 
     * @param maxStringParamLength maximum length allowed for string parameters
     * @param batchSize number of operations to batch together (1 = no batching)
     * @param autoCommit whether to automatically commit transactions
     * @throws IllegalArgumentException if maxStringParamLength or batchSize is not positive
     */
    public StorageConfig(int maxStringParamLength, int batchSize, boolean autoCommit) {
        if (maxStringParamLength <= 0) {
            throw new IllegalArgumentException("Max string param length must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
        this.maxStringParamLength = maxStringParamLength;
        this.batchSize = batchSize;
        this.autoCommit = autoCommit;
    }

    /**
     * Returns the maximum length allowed for string parameters.
     * 
     * <p>String parameters longer than this limit will cause an
     * {@link it.fvaleri.sqlrunner.exception.InvalidParameterException} to be thrown.</p>
     * 
     * @return the maximum string parameter length in characters
     */
    public int getMaxStringParamLength() {
        return maxStringParamLength;
    }

    /**
     * Returns the batch size for batch operations.
     * 
     * <p>When batch size is greater than 1, operations will be accumulated
     * and executed together when the batch is full. A batch size of 1
     * means no batching (operations execute immediately).</p>
     * 
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Returns whether auto-commit is enabled.
     * 
     * <p>When auto-commit is enabled (true), each operation is automatically
     * committed. When disabled (false), transactions must be manually
     * committed or rolled back.</p>
     * 
     * @return true if auto-commit is enabled, false otherwise
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Creates a new Builder for constructing StorageConfig instances.
     * 
     * @return a new Builder instance with default values
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating StorageConfig instances.
     * 
     * <p>Provides a fluent API for configuring storage options:</p>
     * <pre>{@code
     * StorageConfig config = StorageConfig.builder()
     *     .maxStringParamLength(500)
     *     .batchSize(50)
     *     .autoCommit(false)
     *     .build();
     * }</pre>
     */
    public static class Builder {
        private int maxStringParamLength = DEFAULT_MAX_STRING_PARAM_LENGTH;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private boolean autoCommit = DEFAULT_AUTO_COMMIT;

        /**
         * Sets the maximum length for string parameters.
         * 
         * @param maxStringParamLength maximum length in characters (must be positive)
         * @return this Builder instance for method chaining
         * @throws IllegalArgumentException if maxStringParamLength is not positive (during build())
         */
        public Builder maxStringParamLength(int maxStringParamLength) {
            this.maxStringParamLength = maxStringParamLength;
            return this;
        }

        /**
         * Sets the batch size for batch operations.
         * 
         * @param batchSize number of operations to batch (1 = no batching, must be positive)
         * @return this Builder instance for method chaining
         * @throws IllegalArgumentException if batchSize is not positive (during build())
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the auto-commit behavior.
         * 
         * @param autoCommit true to automatically commit transactions, false for manual control
         * @return this Builder instance for method chaining
         */
        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        /**
         * Builds a new StorageConfig instance with the configured values.
         * 
         * @return a new StorageConfig instance
         * @throws IllegalArgumentException if maxStringParamLength or batchSize is not positive
         */
        public StorageConfig build() {
            return new StorageConfig(maxStringParamLength, batchSize, autoCommit);
        }
    }
}