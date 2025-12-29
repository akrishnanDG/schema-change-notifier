package io.confluent.schemachange.state;

import io.confluent.schemachange.config.AppConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DeduplicationService.
 */
class DeduplicationServiceTest {

    @TempDir
    Path tempDir;

    private DeduplicationService service;
    private AppConfig config;

    @BeforeEach
    void setUp() {
        config = new AppConfig();
        config.setStateFilePath(tempDir.resolve("dedup-state.json").toString());
        service = new DeduplicationService(config);
    }

    @AfterEach
    void tearDown() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    @DisplayName("New key should not be a duplicate")
    void testIsDuplicate_NewKey() {
        assertFalse(service.isDuplicate("new-key"));
    }

    @Test
    @DisplayName("Marked key should be a duplicate")
    void testIsDuplicate_MarkedKey() {
        service.markProcessed("test-key");

        assertTrue(service.isDuplicate("test-key"));
    }

    @Test
    @DisplayName("markProcessed should return true for new key")
    void testMarkProcessed_NewKey() {
        assertTrue(service.markProcessed("new-key"));
    }

    @Test
    @DisplayName("markProcessed should return false for existing key")
    void testMarkProcessed_ExistingKey() {
        service.markProcessed("test-key");

        assertFalse(service.markProcessed("test-key"));
    }

    @Test
    @DisplayName("size should return number of tracked events")
    void testSize() {
        assertEquals(0, service.size());

        service.markProcessed("key1");
        service.markProcessed("key2");
        service.markProcessed("key3");

        assertEquals(3, service.size());
    }

    @Test
    @DisplayName("clear should remove all tracked events")
    void testClear() {
        service.markProcessed("key1");
        service.markProcessed("key2");
        assertEquals(2, service.size());

        service.clear();

        assertEquals(0, service.size());
        assertFalse(service.isDuplicate("key1"));
    }

    @Test
    @DisplayName("State should persist across service restarts")
    void testStatePersistence() throws IOException {
        service.markProcessed("persistent-key-1");
        service.markProcessed("persistent-key-2");
        service.close();

        // Create a new service with the same config
        DeduplicationService newService = new DeduplicationService(config);

        assertTrue(newService.isDuplicate("persistent-key-1"));
        assertTrue(newService.isDuplicate("persistent-key-2"));
        assertEquals(2, newService.size());

        newService.close();
    }

    @Test
    @DisplayName("Service should start fresh if state file doesn't exist")
    void testLoadState_NoFile() {
        Path nonExistentPath = tempDir.resolve("non-existent.json");
        config.setStateFilePath(nonExistentPath.toString());

        DeduplicationService freshService = new DeduplicationService(config);

        assertEquals(0, freshService.size());
        freshService.close();
    }

    @Test
    @DisplayName("Service should handle corrupted state file gracefully")
    void testLoadState_CorruptedFile() throws IOException {
        Path corruptedPath = tempDir.resolve("corrupted.json");
        Files.writeString(corruptedPath, "not valid json {{{");
        config.setStateFilePath(corruptedPath.toString());

        DeduplicationService corruptedService = new DeduplicationService(config);

        // Should start fresh without throwing
        assertEquals(0, corruptedService.size());
        corruptedService.close();
    }

    @Test
    @DisplayName("Multiple keys should be tracked independently")
    void testMultipleKeys() {
        service.markProcessed("subject1:RegisterSchema:100001");
        service.markProcessed("subject2:RegisterSchema:100002");
        service.markProcessed("subject1:DeleteSchema:100003");

        assertTrue(service.isDuplicate("subject1:RegisterSchema:100001"));
        assertTrue(service.isDuplicate("subject2:RegisterSchema:100002"));
        assertTrue(service.isDuplicate("subject1:DeleteSchema:100003"));
        assertFalse(service.isDuplicate("subject1:RegisterSchema:100002"));
    }

    @Test
    @DisplayName("State file should be created in specified directory")
    void testStateFileCreation() {
        Path nestedPath = tempDir.resolve("nested/dir/state.json");
        config.setStateFilePath(nestedPath.toString());

        DeduplicationService nestedService = new DeduplicationService(config);
        nestedService.markProcessed("test-key");
        nestedService.close();

        assertTrue(Files.exists(nestedPath));
    }

    @Test
    @DisplayName("Concurrent access should be thread-safe")
    void testConcurrentAccess() throws InterruptedException {
        int numThreads = 10;
        int keysPerThread = 100;
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < keysPerThread; i++) {
                    service.markProcessed("thread" + threadId + "-key" + i);
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(numThreads * keysPerThread, service.size());
    }
}

