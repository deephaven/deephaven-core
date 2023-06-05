/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import java.time.Instant;

abstract class BaseMessageMetadata implements MessageMetadata {

    // The number of metadata fields that need separate setters.
    protected static final int NUM_METADATA_FIELDS = 3;

    private final Instant sentTime;
    private final Instant receiveTime;
    private final Instant ingestTime;
    private final String messageId;
    private final long msgNo;

    /**
     * Create a new instance of this class.
     * 
     * @param sentTime The time (if available) when this message was published
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be flushed.
     * @param messageId An optional message ID string. (Used by some message brokers to support recovery.)
     * @param messageNumber The unique, monotonically-increasing sequential number for this message.
     */
    public BaseMessageMetadata(
            final Instant sentTime,
            final Instant receiveTime,
            final Instant ingestTime,
            final String messageId,
            final long messageNumber) {
        this.sentTime = sentTime;
        this.receiveTime = receiveTime;
        this.ingestTime = ingestTime;
        this.messageId = messageId;
        this.msgNo = messageNumber;
    }

    @Override
    public Instant getSentTime() {
        return sentTime;
    }

    @Override
    public Instant getReceiveTime() {
        return receiveTime;
    }

    @Override
    public Instant getIngestTime() {
        return ingestTime;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public long getMsgNo() {
        return msgNo;
    }

}
