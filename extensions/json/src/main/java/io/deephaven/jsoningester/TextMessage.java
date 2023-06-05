/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import java.time.Instant;

/**
 * An internal storage for text message contents plus metadata in a standardized format for this plugin to further
 * process.
 **/
public class TextMessage extends BaseMessageMetadata {
    private final String text;

    /**
     * Create a new instance of this class.
     * 
     * @param sentTime The time (if available) when this message was sent.
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be flushed.
     * @param messageId An optional message ID string. (Used by some message brokers to support recovery.)
     * @param messageNumber The monotonically-increasing sequence number for the message.
     * @param text The String message body.
     */
    public TextMessage(final Instant sentTime,
            final Instant receiveTime,
            final Instant ingestTime,
            final String messageId,
            final long messageNumber,
            final String text) {
        super(sentTime, receiveTime, ingestTime, messageId, messageNumber);
        this.text = text;
    }

    /**
     * Gets the text body of the message.
     * 
     * @return A string of the message body.
     */
    public String getText() {
        return text;
    }

    /**
     * Gets the number of metadata fields included in this type of MessageMetadata object.
     * 
     * @return the number of metadata fields included in this type of MessageMetadata object.
     */
    public static int numberOfMetadataFields() {
        return NUM_METADATA_FIELDS + 1;
    }
}
