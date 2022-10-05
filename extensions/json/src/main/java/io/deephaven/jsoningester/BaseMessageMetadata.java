/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.time.DateTime;

abstract class BaseMessageMetadata implements MessageMetadata {

    // The number of metadata fields that need separate setters.
    protected static final int NUM_METADATA_FIELDS = 3;

    private final DateTime sentTime;
    private final DateTime receiveTime;
    private final DateTime ingestTime;
    private final String messageId;
    private final long msgNo;

    /**
     * Create a new instance of this class.
     * 
     * @param sentTime The time (if available) when this message was published
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be written
     *        to disk.
     * @param messageId The unique, monotonically-increasing ID for this message.
     * @param messageNumber The sequential number indicating the sequence this message was received in by the ingester.
     */
    public BaseMessageMetadata(final DateTime sentTime, final DateTime receiveTime, final DateTime ingestTime,
            final String messageId, final long messageNumber) {
        this.sentTime = sentTime;
        this.receiveTime = receiveTime;
        this.ingestTime = ingestTime;
        this.messageId = messageId;
        this.msgNo = messageNumber;
    }

    @Override
    public DateTime getSentTime() {
        return sentTime;
    }

    @Override
    public DateTime getReceiveTime() {
        return receiveTime;
    }

    @Override
    public DateTime getIngestTime() {
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
