package io.confluent.schemachange.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Executes operations with exponential backoff retry logic.
 * Only retries on transient {@link IOException} failures.
 */
public class RetryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RetryExecutor.class);

    private final int maxRetries;
    private final long initialBackoffMs;
    private final long maxBackoffMs;

    public RetryExecutor(int maxRetries, long initialBackoffMs, long maxBackoffMs) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
    }

    /**
     * Executes the given action with retry and exponential backoff.
     * Only retries on {@link IOException}. All other exceptions propagate immediately.
     *
     * @param action        The operation to execute
     * @param operationName A descriptive name for logging
     * @param <T>           The return type
     * @return The result of the action
     * @throws Exception if all retries are exhausted or a non-retryable exception occurs
     */
    public <T> T executeWithRetry(@Nonnull Callable<T> action, @Nonnull String operationName) throws Exception {
        int attempt = 0;
        long backoffMs = initialBackoffMs;

        while (true) {
            try {
                return action.call();
            } catch (IOException e) {
                attempt++;
                if (attempt > maxRetries) {
                    logger.error("Operation '{}' failed after {} retries: {}", operationName, maxRetries, e.getMessage());
                    throw e;
                }
                logger.warn("Operation '{}' failed (attempt {}/{}), retrying in {}ms: {}",
                        operationName, attempt, maxRetries, backoffMs, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
            }
        }
    }
}
