package io.deephaven.kafka.ingest;

import io.deephaven.UncheckedDeephavenException;

/**
 * This exception is thrown when there is a failure to consume a Kafka record during Kafka to Deephaven ingestion.
 */
public class KafkaIngesterException extends UncheckedDeephavenException {
    /**
     * Constructs a new KafkaIngesterException with the specified reason.
     *
     * @param reason the exception detail message
     * @param cause  the exception cause
     */
    public KafkaIngesterException(String reason, Exception cause) {
        super(reason, cause);
    }

    /**
     * Constructs a new KafkaIngesterException with the specified reason.
     *
     * @param reason the exception detail message
     */
    public KafkaIngesterException(String reason) {
        super(reason);
    }
}
