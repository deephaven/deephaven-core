package io.deephaven.jsoningester;

import io.deephaven.time.DateTime;

/**
 * Created by rbasralian on 10/1/22
 */
public interface MessageMetadata {

    /**
     * Gets the time (if available) when this message was published.
     *
     * @return the time (if available) when this message was published.
     */
    DateTime getSentTime();

    /**
     * Gets the time (reported by subscriber) when this message was received.
     *
     * @return the time (reported by subscriber) when this message was received.
     */
    DateTime getReceiveTime();

    /**
     * Gets the time when this message was finished processing by its ingester
     * and was ready to be written to disk.
     *
     * @return the time when this message was finished processing by its ingester
     * and was ready to be written to disk.
     */
    DateTime getIngestTime();

    /**
     * Gets the unique, monotonically-increasing ID for this message.
     *
     * @return he unique, monotonically-increasing ID for this message.
     */
    String getMessageId();

    /**
     * Gets the sequential number indicating the sequence this message was
     * received in by the ingester.
     *
     * @return the sequential number indicating the sequence this message was
     * received in by the ingester.
     */
    long getMsgNo();
}
