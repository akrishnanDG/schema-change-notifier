package io.confluent.schemachange.exception;

/**
 * Exception thrown when notification production fails.
 * This includes Kafka producer errors, serialization failures, and schema registration issues.
 */
public class NotificationProducerException extends RuntimeException {

    private final String subject;

    /**
     * Creates a new NotificationProducerException.
     *
     * @param message The error message
     */
    public NotificationProducerException(String message) {
        super(message);
        this.subject = null;
    }

    /**
     * Creates a new NotificationProducerException with a cause.
     *
     * @param message The error message
     * @param cause   The underlying cause
     */
    public NotificationProducerException(String message, Throwable cause) {
        super(message, cause);
        this.subject = null;
    }

    /**
     * Creates a new NotificationProducerException for a specific subject.
     *
     * @param message The error message
     * @param subject The schema subject that failed
     * @param cause   The underlying cause
     */
    public NotificationProducerException(String message, String subject, Throwable cause) {
        super(message, cause);
        this.subject = subject;
    }

    /**
     * @return The schema subject that failed, or null if not applicable
     */
    public String getSubject() {
        return subject;
    }
}

