package io.deephaven.stream;

/**
 * The result returned by {@link StreamConsumer#accept(io.deephaven.db.v2.sources.chunk.WritableChunk[])} and similar
 * methods, used to advise the enclosing stack of the consumer's status.
 */
public enum StreamConsumerResult {
    /**
     * Returned when the consumer is functioning as normal and available to process subsequent records.
     */
    ACTIVE,
    /**
     * Returned when the consumer has failed to process a previous record and will not process any subsequent records.
     */
    FAILED,
    /**
     * Returned when the consumer is done and will not process any subsequent records.
     */
    COMPLETED
}
