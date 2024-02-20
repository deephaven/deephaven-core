/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.jsoningester.msg.MessageMetadata;
import io.deephaven.jsoningester.msg.TextMessage;

import java.io.IOException;

/**
 * String messages must be adapted to an ingester, such as a {@link io.deephaven.tablelogger.TableWriter} or
 * {@link io.deephaven.stream.StreamPublisher}. The StringIngestionAdapter consumes a String message and writes zero or
 * more rows to the ingester.
 */
public interface StringIngestionAdapter extends AsynchronousDataIngester {
    /**
     * Consume a generic String and write zero or more records to an ingeseter (e.g.
     * {@link io.deephaven.tablelogger.TableWriter} or {@link io.deephaven.stream.StreamPublisher}).
     *
     * @param msg The message to be consumed, including the string paylod and metadata. The
     *        {@link MessageMetadata#getMsgNo() message number} (and, if present, {@link MessageMetadata#getMessageId()
     *        message ID}) must be unique and increasing for any source using this method to write data to a Deephaven
     *        table.
     * @throws IOException if there was an error writing to the output table
     */
    void consumeString(final TextMessage msg) throws IOException;

    /**
     * Record the owning StringMessageToTableAdapter, if this StringIngestionAdapter needs that.
     * 
     * @param parent The StringMessageToTableAdapter that controls this StringIngestionAdapter.
     */
    void setOwner(StringMessageToTableAdapter<?> parent);

}
