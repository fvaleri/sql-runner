/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StorageConfigTest {
    @Test
    void shouldCreateDefaultConfig() {
        StorageConfig config = new StorageConfig();
        
        assertEquals(100, config.getMaxStringParamLength());
        assertEquals(1, config.getBatchSize());
        assertTrue(config.isAutoCommit());
    }

    @Test
    void shouldCreateConfigWithCustomValues() {
        StorageConfig config = new StorageConfig(200, 5, false);
        
        assertEquals(200, config.getMaxStringParamLength());
        assertEquals(5, config.getBatchSize());
        assertFalse(config.isAutoCommit());
    }


    @Test
    void shouldFailWithInvalidMaxStringParamLength() {
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> new StorageConfig(0, 1, true));
        assertEquals("Max string param length must be positive", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> new StorageConfig(-1, 1, true));
        assertEquals("Max string param length must be positive", e2.getMessage());
    }

    @Test
    void shouldFailWithInvalidBatchSize() {
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> new StorageConfig(100, 0, true));
        assertEquals("Batch size must be positive", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> new StorageConfig(100, -1, true));
        assertEquals("Batch size must be positive", e2.getMessage());
    }

    @Test
    void shouldBuildWithDefaults() {
        StorageConfig config = StorageConfig.builder().build();
        
        assertEquals(100, config.getMaxStringParamLength());
        assertEquals(1, config.getBatchSize());
        assertTrue(config.isAutoCommit());
    }

    @Test
    void shouldBuildWithCustomValues() {
        StorageConfig config = StorageConfig.builder()
                .maxStringParamLength(500)
                .batchSize(10)
                .autoCommit(false)
                .build();
        
        assertEquals(500, config.getMaxStringParamLength());
        assertEquals(10, config.getBatchSize());
        assertFalse(config.isAutoCommit());
    }

    @Test
    void shouldChainBuilderMethods() {
        StorageConfig config = StorageConfig.builder()
                .maxStringParamLength(300)
                .maxStringParamLength(400) // should override previous value
                .batchSize(8)
                .autoCommit(false)
                .autoCommit(true) // should override previous value
                .build();
        
        assertEquals(400, config.getMaxStringParamLength());
        assertEquals(8, config.getBatchSize());
        assertTrue(config.isAutoCommit());
    }

    @Test
    void shouldFailBuilderWithInvalidValues() {
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> 
                StorageConfig.builder().maxStringParamLength(-1).build());
        assertEquals("Max string param length must be positive", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> 
                StorageConfig.builder().batchSize(0).build());
        assertEquals("Batch size must be positive", e2.getMessage());
    }
}
