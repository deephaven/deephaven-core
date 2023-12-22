/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.streampublisher;

import io.deephaven.UncheckedDeephavenException;

/**
 * This exception is thrown when there is a failure to consume a Kafka record during Kafka to Deephaven ingestion.
 */
public class IngesterException extends UncheckedDeephavenException {
    /**
     * Constructs a new KafkaIngesterException with the specified reason.
     *
     * @param reason the exception detail message
     * @param cause the exception cause
     */
    public IngesterException(String reason, Throwable cause) {
        super(reason, cause);
    }

    /**
     * Constructs a new KafkaIngesterException with the specified reason.
     *
     * @param reason the exception detail message
     */
    public IngesterException(String reason) {
        super(reason);
    }
}
