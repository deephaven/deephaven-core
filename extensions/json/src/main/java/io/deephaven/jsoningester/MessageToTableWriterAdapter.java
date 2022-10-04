/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.tablelogger.TableWriter;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Messages are externally produced, which we must then adapt to a {@link TableWriter TableWriter}.
 * <p>
 * The {@code MessagesToTableWriterAdapter} consumes a message of type {@code M} and writes zero or more rows to
 * a TableWriter. This adapter extracts an appropriate Java data type (e.g. a string or object) from the message,
 * then passes the message content and metadata (e.g. timestamps) to a {@link DataToTableWriterAdapter} to write
 * the message data to the TableWriter.
 *
 * <pre>
 *     Subscriber --[message of type 'M']--> MessageToTableWriterAdapter --[String/Object]--> DataToTableWriterAdapter --> TableWriter
 * </pre>
 *
 * @param <M> Message object type
 */
public interface MessageToTableWriterAdapter<M> {
    /**
     * Consume a generic String and write zero or more records to a TableWriter.
     *
     * @param msgId   The ID associated with the string to be consumed. ID must be unique and increasing for any source
     *                using this method to write data to a Deephaven database.
     * @param message The received message
     * @throws IOException if there was an error writing to the output table
     */
    void consumeMessage(String msgId, M message) throws IOException;

    /**
     * Take care of any 'cleanup' tasks that might be waiting; this is expected to mean that the inbound data has
     * hit a lull and there is time to do this.
     *
     * @throws IOException If there was an error writing to the output table during cleanup.
     */
    void cleanup() throws IOException;

    /**
     * Wait for all enqueued messages to finish processing. If additional messages are queued after this function is
     * called, those messages may not be processed prior to returning. Additionally, this does not guarantee that all
     * processed messages are written. cleanup should be called subsequent to this function to ensure that.
     *
     * @param timeoutMillis maximum time to wait in milliseconds
     * @throws InterruptedException if wait was interrupted
     */
    void waitForProcessing(long timeoutMillis) throws InterruptedException, TimeoutException;

    /**
     * Return the last message ID processed by this adapter.
     *
     * @return null if there were no messages ever written to the location being used by this
     * adapter; otherwise, returns the String last checkpoint ID.
     */
    String getLastMessageId();

//    /**
//     * Sets a {@link SimpleDataImportStreamProcessor} to be used when the adapter
//     * will write rows for embedded objects to other tables from the one used for
//     * the top-level message properties.
//     * @param processor The {@link SimpleDataImportStreamProcessor} typically initialized
//     *                  from {@link EmbeddedObjectAdapter#getEmbeddedObjectAdapter()};
//     * @param lastCheckpointId A String value of the last checkpoint ID read from a
//     *                         file on startup of the processor. Will be null for a
//     *                         new file.
//     */
//    void setProcessor(@NotNull final SimpleDataImportStreamProcessor processor,
//                      final String lastCheckpointId);

    /**
     * Shuts down the adapter, including the {@link DataToTableWriterAdapter table writer adapters}.
     */
    void shutdown();
}
