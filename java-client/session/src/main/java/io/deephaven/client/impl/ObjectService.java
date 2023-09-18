/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectService {
    /**
     * Fetch the object.
     *
     * @param type the type, must be non-null
     * @param ticket the ticket
     * @return the future
     */
    CompletableFuture<FetchedObject> fetchObject(String type, HasTicketId ticket);

    /**
     * Fetch the object represented by the {@code typedTicket}. The type must be present.
     *
     * @param typedTicket the typed ticket
     * @return the future
     */
    CompletableFuture<FetchedObject> fetchObject(HasTypedTicket typedTicket);

    /**
     * The sending and receiving interface for {@link #messageStream(HasTypedTicket, MessageStream)}.
     *
     * @param <Ref> the reference type
     */
    interface MessageStream<Ref> {
        void onData(ByteBuffer payload, List<? extends Ref> references);

        void onClose();
    }

    /**
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
     * @param stream the stream where the client will receive messages
     * @return the stream where the client will send messages
     */
    MessageStream<HasTypedTicket> messageStream(HasTypedTicket typedTicket, MessageStream<ServerObject> stream);
}
