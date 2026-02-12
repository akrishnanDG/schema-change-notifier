package io.confluent.schemachange.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CircuitBreaker Tests")
class CircuitBreakerTest {

    private CircuitBreaker breaker;

    @BeforeEach
    void setUp() {
        breaker = new CircuitBreaker("test", 3, 200);
    }

    @Test
    @DisplayName("Should start in CLOSED state")
    void shouldStartClosed() {
        assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
        assertEquals(0, breaker.getFailureCount());
    }

    @Test
    @DisplayName("Should allow requests in CLOSED state")
    void shouldAllowRequestsWhenClosed() throws Exception {
        String result = breaker.execute(() -> "success");
        assertEquals("success", result);
        assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
    }

    @Test
    @DisplayName("Should transition to OPEN after failure threshold")
    void shouldOpenAfterFailureThreshold() {
        for (int i = 0; i < 3; i++) {
            assertThrows(IOException.class, () ->
                    breaker.execute(() -> { throw new IOException("fail"); }));
        }
        assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
        assertEquals(3, breaker.getFailureCount());
    }

    @Test
    @DisplayName("Should reject requests when OPEN")
    void shouldRejectWhenOpen() {
        // Trip the breaker
        for (int i = 0; i < 3; i++) {
            assertThrows(IOException.class, () ->
                    breaker.execute(() -> { throw new IOException("fail"); }));
        }

        // Should throw CircuitBreakerOpenException
        assertThrows(CircuitBreaker.CircuitBreakerOpenException.class, () ->
                breaker.execute(() -> "should not reach"));
    }

    @Test
    @DisplayName("Should stay CLOSED below failure threshold")
    void shouldStayClosedBelowThreshold() {
        for (int i = 0; i < 2; i++) {
            assertThrows(IOException.class, () ->
                    breaker.execute(() -> { throw new IOException("fail"); }));
        }
        assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
        assertEquals(2, breaker.getFailureCount());
    }

    @Test
    @DisplayName("Should reset failure count on success")
    void shouldResetFailureCountOnSuccess() throws Exception {
        // Two failures
        assertThrows(IOException.class, () ->
                breaker.execute(() -> { throw new IOException("fail"); }));
        assertThrows(IOException.class, () ->
                breaker.execute(() -> { throw new IOException("fail"); }));
        assertEquals(2, breaker.getFailureCount());

        // One success resets
        breaker.execute(() -> "ok");
        assertEquals(0, breaker.getFailureCount());
        assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
    }

    @Test
    @DisplayName("Should transition to HALF_OPEN after reset timeout")
    void shouldTransitionToHalfOpenAfterTimeout() throws Exception {
        // Use very short reset timeout
        CircuitBreaker shortBreaker = new CircuitBreaker("short", 2, 50);

        // Trip the breaker
        for (int i = 0; i < 2; i++) {
            assertThrows(IOException.class, () ->
                    shortBreaker.execute(() -> { throw new IOException("fail"); }));
        }
        assertEquals(CircuitBreaker.State.OPEN, shortBreaker.getState());

        // Wait for reset timeout
        Thread.sleep(100);

        // Next call should go through (HALF_OPEN)
        String result = shortBreaker.execute(() -> "recovered");
        assertEquals("recovered", result);
        assertEquals(CircuitBreaker.State.CLOSED, shortBreaker.getState());
    }

    @Test
    @DisplayName("Should go back to OPEN if HALF_OPEN request fails")
    void shouldReopenIfHalfOpenFails() throws Exception {
        CircuitBreaker shortBreaker = new CircuitBreaker("short", 2, 50);

        // Trip the breaker
        for (int i = 0; i < 2; i++) {
            assertThrows(IOException.class, () ->
                    shortBreaker.execute(() -> { throw new IOException("fail"); }));
        }

        // Wait for reset timeout
        Thread.sleep(100);

        // HALF_OPEN attempt fails
        assertThrows(IOException.class, () ->
                shortBreaker.execute(() -> { throw new IOException("still failing"); }));

        assertEquals(CircuitBreaker.State.OPEN, shortBreaker.getState());
    }

    @Test
    @DisplayName("CircuitBreakerOpenException should have message")
    void circuitBreakerOpenExceptionShouldHaveMessage() {
        CircuitBreaker.CircuitBreakerOpenException ex =
                new CircuitBreaker.CircuitBreakerOpenException("test message");
        assertEquals("test message", ex.getMessage());
    }
}
