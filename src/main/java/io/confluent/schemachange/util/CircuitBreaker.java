package io.confluent.schemachange.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple circuit breaker to protect against cascading failures when calling external services.
 * <p>
 * States:
 * <ul>
 *   <li>CLOSED - Normal operation, requests pass through</li>
 *   <li>OPEN - Failing, requests are rejected immediately</li>
 *   <li>HALF_OPEN - Testing, one request is allowed through to test recovery</li>
 * </ul>
 */
public class CircuitBreaker {

    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final String name;
    private final int failureThreshold;
    private final long resetTimeoutMs;
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);

    public CircuitBreaker(String name, int failureThreshold, long resetTimeoutMs) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    /**
     * Executes the given action through the circuit breaker.
     *
     * @param action The operation to execute
     * @param <T>    The return type
     * @return The result of the action
     * @throws CircuitBreakerOpenException if the circuit is open
     * @throws Exception                   if the action throws
     */
    public <T> T execute(Callable<T> action) throws Exception {
        State currentState = state.get();

        if (currentState == State.OPEN) {
            if (shouldAttemptReset()) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    logger.info("Circuit breaker '{}' transitioning from OPEN to HALF_OPEN", name);
                }
                return tryExecution(action);
            }
            throw new CircuitBreakerOpenException(
                    "Circuit breaker '" + name + "' is OPEN, rejecting request");
        }

        return tryExecution(action);
    }

    private <T> T tryExecution(Callable<T> action) throws Exception {
        try {
            T result = action.call();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure();
            throw e;
        }
    }

    private void onSuccess() {
        if (state.get() != State.CLOSED) {
            logger.info("Circuit breaker '{}' transitioning to CLOSED", name);
        }
        failureCount.set(0);
        state.set(State.CLOSED);
    }

    private void onFailure() {
        lastFailureTime.set(System.currentTimeMillis());
        int failures = failureCount.incrementAndGet();

        if (state.get() == State.HALF_OPEN) {
            state.set(State.OPEN);
            logger.warn("Circuit breaker '{}' transitioning from HALF_OPEN to OPEN", name);
        } else if (failures >= failureThreshold) {
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                logger.warn("Circuit breaker '{}' transitioning from CLOSED to OPEN after {} failures",
                        name, failures);
            }
        }
    }

    private boolean shouldAttemptReset() {
        return System.currentTimeMillis() - lastFailureTime.get() >= resetTimeoutMs;
    }

    public State getState() {
        return state.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    /**
     * Exception thrown when the circuit breaker is open and rejecting requests.
     */
    public static class CircuitBreakerOpenException extends Exception {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}
