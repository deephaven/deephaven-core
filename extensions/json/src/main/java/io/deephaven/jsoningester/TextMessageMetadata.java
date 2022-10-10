/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.time.DateTime;

/**
 * An internal storage for text message contents plus metadata in a standardized format for this plugin to further
 * process.
 **/
public class TextMessageMetadata extends BaseMessageMetadata {
    private final String text;

    /**
     * Create a new instance of this class.
     * 
     * @param sentTime The time (if available) when this message was sent
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be flushed.
     * @param messageId The unique, monotonically-increasing ID for this message.
     * @param messageNumber The sequential number indicating the sequence this message was received in by the ingester.
     * @param text The String message body.
     */
    public TextMessageMetadata(final DateTime sentTime, final DateTime receiveTime, final DateTime ingestTime,
            final String messageId, final long messageNumber, final String text) {
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
