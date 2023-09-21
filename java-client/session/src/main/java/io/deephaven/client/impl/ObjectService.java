/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface ObjectService {

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
         * Fetches {@code this}. The resulting object is managed separately from {@code this}.
         *
         * @return the future
         * @see Session#fetch(HasTypedTicket) for the lower-level interface
         */
        CompletableFuture<ServerData> fetch();
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
         * Initiates a connection for a bidirectional message stream for {@code this}. The returned {@link ServerData}
         * messages are managed separately from {@code this}.
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
         * @param receiveStream the stream where the client will receive messages
         * @return the stream where the client will send messages
         * @see Session#connect(HasTypedTicket, MessageStream) for the lower-level interface
         */
        MessageStream<ClientData> connect(MessageStream<ServerData> receiveStream);
    }

    /**
     * The sending and receiving interface for {@link #connect(HasTypedTicket, MessageStream)}.
     *
     * @param <Message> the message type
     */
    interface MessageStream<Message> {
        void onData(Message message);

        void onClose();
    }

    /**
     * Exports {@code typedTicket} to a client-managed fetchable server object.
     *
     * @param typedTicket the typed ticket
     * @return the future
     * @see Session#export(HasTypedTicket)
     */
    CompletableFuture<? extends Fetchable> fetchable(HasTypedTicket typedTicket);

    /**
     * Exports {@code typedTicket} to a client-managed bidirectional server object.
     *
     * @param typedTicket the typed ticket
     * @return the future
     * @see Session#export(HasTypedTicket)
     */
    CompletableFuture<? extends Bidirectional> bidirectional(HasTypedTicket typedTicket);

    /**
     * The low-level interface for fetching data. See {@link #fetchable(HasTypedTicket)} for a higher-level interface.
     *
     * @param typedTicket the typed ticket
     * @return the future
     */
    CompletableFuture<ServerData> fetch(HasTypedTicket typedTicket);

    /**
     * The low-level interface for initiating a connection for a bidirectional message stream for {@code typedTicket}.
     * See {@link #bidirectional(HasTypedTicket)} for a higher-level interface.
     *
     * <p>
     * Opens a bidirectional message stream for a {@code typedTicket}. References sent to the server are generic
     * {@link HasTypedTicket typed tickets}, while the references received from the server are {@link ServerObject
     * server objects}. The caller is responsible for {@link ServerObject#release() releasing} or
     * {@link ServerObject#close() closing} the server objects.
     *
     * <p>
     * This provides a generic stream feature for Deephaven instances to use to add arbitrary functionality. This API
     * lets a client open a stream to a particular object on the server, to be mediated by a server side plugin. In
     * theory this could effectively be used to "tunnel" a custom gRPC call, but in practice there are a few deliberate
     * shortcomings that still make this possible, but not trivial.
     *
     * <p>
     * Presently it is required that the server respond immediately, at least to acknowledge that the object was
     * correctly contacted (as opposed to waiting for a pending ticket, or dealing with network lag, etc). This is a
     * small (and possibly not required, but convenient) departure from a offering a gRPC stream (a server-streaming or
     * bidi-streaming call need not send a message right away).
     *
     * @param typedTicket the typed ticket
     * @param receiveStream the stream where the client will receive messages
     * @return the stream where the client will send messages
     */
    MessageStream<ClientData> connect(HasTypedTicket typedTicket, MessageStream<ServerData> receiveStream);
}
