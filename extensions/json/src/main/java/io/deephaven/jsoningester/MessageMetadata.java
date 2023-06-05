package io.deephaven.jsoningester;

import java.time.Instant;

/**
 * Interface for providers of message metadata (e.g. timestamps, a {@link #getMsgNo() sequence number}, and a
 * {@link #getMessageId() message ID}).
 */
public interface MessageMetadata {

    /**
     * Gets the time (if available) when this message was published.
     *
     * @return the time (if available) when this message was published.
     */
    Instant getSentTime();

    /**
     * Gets the time (reported by subscriber) when this message was received.
     *
     * @return the time (reported by subscriber) when this message was received.
     */
    Instant getReceiveTime();

    /**
     * Gets the time when this message was finished processing by its ingester and was ready to be flushed.
     *
     * @return the time when this message was finished processing by its ingester and was ready to be flushed.
     */
    Instant getIngestTime();

    /**
     * Gets the ID for this message.
     *
     * @return the ID for this message.
     */
    String getMessageId();

    /**
     * Gets the monotonically-increasing sequence number for this message, indicating the order in which this message
     * was received in by the ingester. The message number is used to ensure that data is written to the table in the
     * order in which it was received, particularly when parallel processing is enabled.
     *
     * @return This message's sequence number.
     */
    long getMsgNo();
}
