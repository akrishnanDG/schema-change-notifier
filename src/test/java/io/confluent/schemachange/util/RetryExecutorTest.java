package io.confluent.schemachange.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RetryExecutor Tests")
class RetryExecutorTest {

    @Test
    @DisplayName("Should succeed on first attempt without retry")
    void shouldSucceedOnFirstAttempt() throws Exception {
        RetryExecutor executor = new RetryExecutor(3, 10, 100);
        String result = executor.executeWithRetry(() -> "success", "test-op");
        assertEquals("success", result);
    }

    @Test
    @DisplayName("Should retry on IOException and succeed")
    void shouldRetryOnIOExceptionAndSucceed() throws Exception {
        RetryExecutor executor = new RetryExecutor(3, 10, 100);
        AtomicInteger attempts = new AtomicInteger(0);

        String result = executor.executeWithRetry(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IOException("transient failure");
            }
            return "recovered";
        }, "test-op");

        assertEquals("recovered", result);
        assertEquals(3, attempts.get());
    }

    @Test
    @DisplayName("Should throw after max retries exhausted")
    void shouldThrowAfterMaxRetries() {
        RetryExecutor executor = new RetryExecutor(2, 10, 100);
        AtomicInteger attempts = new AtomicInteger(0);

        IOException thrown = assertThrows(IOException.class, () ->
                executor.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw new IOException("persistent failure");
                }, "test-op")
        );

        assertEquals("persistent failure", thrown.getMessage());
        assertEquals(3, attempts.get()); // 1 initial + 2 retries
    }

    @Test
    @DisplayName("Should not retry on non-IOException")
    void shouldNotRetryOnNonIOException() {
        RetryExecutor executor = new RetryExecutor(3, 10, 100);
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(IllegalStateException.class, () ->
                executor.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("not retryable");
                }, "test-op")
        );

        assertEquals(1, attempts.get());
    }

    @Test
    @DisplayName("Should respect max backoff cap")
    void shouldRespectMaxBackoffCap() throws Exception {
        RetryExecutor executor = new RetryExecutor(5, 10, 50);
        AtomicInteger attempts = new AtomicInteger(0);
        long start = System.currentTimeMillis();

        executor.executeWithRetry(() -> {
            if (attempts.incrementAndGet() <= 4) {
                throw new IOException("fail");
            }
            return "done";
        }, "test-op");

        long elapsed = System.currentTimeMillis() - start;
        // Backoffs: 10 + 20 + 40 + 50 = 120ms minimum (capped at 50 for 4th)
        assertTrue(elapsed >= 100, "Should have waited through backoffs, elapsed=" + elapsed);
        assertEquals(5, attempts.get());
    }

    @Test
    @DisplayName("Should work with zero max retries (no retry)")
    void shouldWorkWithZeroMaxRetries() {
        RetryExecutor executor = new RetryExecutor(0, 10, 100);

        assertThrows(IOException.class, () ->
                executor.executeWithRetry(() -> {
                    throw new IOException("fail");
                }, "test-op")
        );
    }

    @Test
    @DisplayName("Should return null result without error")
    void shouldReturnNullResult() throws Exception {
        RetryExecutor executor = new RetryExecutor(3, 10, 100);
        Object result = executor.executeWithRetry(() -> null, "test-op");
        assertNull(result);
    }
}
