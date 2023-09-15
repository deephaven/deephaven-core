/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ObjectService.MessageStream;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * A server object is a client-owned reference to a server-side object.
 */
public interface ServerObject extends HasExportId, Closeable {

    /**
     * Releases {@code this}. After releasing, callers should not use {@code this} object nor {@link #exportId()} for
     * additional RPC calls.
     *
     * @return the future
     * @see Session#release(ExportId)
     */
    CompletableFuture<Void> release();

    /**
     * Releases {@code this} without waiting for the result. After closing, callers should not use {@code this} object
     * nor {@link #exportId()} for additional RPC calls. For more control, see {@link #release()}.
     */
    @Override
    void close();

    /**
     * A server object that supports a bidirectional message stream.
     */
    interface Bidirectional extends ServerObject {
        /**
         * Opens a bidirectional message stream for {@code this}.
         *
         * @param stream the stream where the client will receive messages
         * @return the stream where the client will send messages
         * @see Session#messageStream(HasTypedTicket, MessageStream)
         */
        MessageStream<HasTypedTicket> messageStream(MessageStream<ServerObject> stream);
    }

    /**
     * A server object that supports fetching.
     */
    interface Fetchable extends ServerObject {
        /**
         * Fetches {@code this}.
         *
         * @return the fetched object
         * @see Session#fetchObject(HasTypedTicket)
         */
        CompletableFuture<FetchedObject> fetch();
    }
}
