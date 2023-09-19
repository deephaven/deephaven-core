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
     * A server object that supports fetching.
     */
    interface Fetchable extends ServerObject {
        /**
         * The type.
         *
         * @return the type
         */
        String type();

        /**
         * Fetches {@code this}. The resulting object is managed separately than {@code this}.
         *
         * @return the future
         * @see Session#fetch(HasTypedTicket) for the lower-level interface
         */
        CompletableFuture<DataAndExports> fetch();
    }

    /**
     * A server object that supports a bidirectional message stream.
     */
    interface Bidirectional extends ServerObject {
        /**
         * The type.
         *
         * @return the type
         */
        String type();

        /**
         * Opens a bidirectional message stream for {@code this}. The returned {@link DataAndExports} messages are
         * managed separately than {@code this}.
         *
         * <p>
         * This provides a generic stream feature for Deephaven instances to use to add arbitrary functionality. This
         * API lets a client open a stream to a particular object on the server, to be mediated by a server side plugin.
         * In theory this could effectively be used to "tunnel" a custom gRPC call, but in practice there are a few
         * deliberate shortcomings that still make this possible, but not trivial.
         *
         * <p>
         * Presently it is required that the server respond immediately, at least to acknowledge that the object was
         * correctly contacted (as opposed to waiting for a pending ticket, or dealing with network lag, etc). This is a
         * small (and possibly not required, but convenient) departure from a offering a gRPC stream (a server-streaming
         * or bidi-streaming call need not send a message right away).
         *
         * @param stream the stream where the client will receive messages
         * @return the stream where the client will send messages
         * @see Session#messageStream(HasTypedTicket, MessageStream) for the lower-level interface
         */
        MessageStream<DataAndTypedTickets> messageStream(MessageStream<DataAndExports> stream);
    }
}
