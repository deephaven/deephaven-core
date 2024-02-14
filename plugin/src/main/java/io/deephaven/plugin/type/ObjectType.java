/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;

import java.nio.ByteBuffer;

/**
 * An "object type" plugin. Useful for serializing custom objects between the server / client.
 */
public interface ObjectType extends Plugin {

    /**
     * The name of the object type.
     *
     * @return the name
     */
    String name();

    /**
     * Returns true if, and only if, the {@code object} is compatible with {@code this} object type.
     *
     * @param object the object
     * @return true if the {@code object} is compatible
     */
    boolean isType(Object object);

    /**
     * A stream of messages, either sent from the server to the client, or client to the server. ObjectType plugin
     * implementations provide an instance of this interface for each incoming stream to invoke as messages arrive, and
     * will likewise be given an instance of this interface to be able to send messages to the client.
     */
    interface MessageStream {
        /**
         * Simple stream that does no handling on data or close.
         */
        MessageStream NOOP = new MessageStream() {
            @Override
            public void onData(ByteBuffer payload, Object... references) {}

            @Override
            public void onClose() {}
        };

        /**
         * Transmits/receives data to/from the remote end of the stream. This can consist of a binary payload and
         * references to objects on the server.
         * <p>
         * </p>
         * When implementing this interface for a plugin, this method will be invoked when a request arrives from the
         * client. When invoking this method from in a plugin, it will send a response to the client.
         * <p>
         * </p>
         * Note that sending a message can cause an exception if there is an error in serializing, and for that reason
         * this method throws a checked exception. It is safe to let that propagate up through either an incoming
         * {@link #onData onData call from a client}, or the {@link ObjectType#clientConnection(Object, MessageStream)
         * call to create the stream}, but it is usually unsafe to let this propagate to other engine threads.
         *
         * @param payload the binary data sent to the remote implementation
         * @param references server-side object references sent to the remote implementation
         * @throws ObjectCommunicationException a checked exception for any errors that may occur, to ensure that the
         *         error is handled without propagating.
         */
        void onData(ByteBuffer payload, Object... references) throws ObjectCommunicationException;

        /**
         * Closes the stream on both ends. No further messages can be sent or received.
         */
        void onClose();
    }

    /**
     * Signals creation of a client stream to the provided object. The returned MessageStream implementation will be
     * called with each received message from the client, and can call the provided connection instance to send messages
     * as needed to the client.
     * 
     * @param object the object to create a connection for
     * @param connection a stream to send objects to the client
     * @throws ObjectCommunicationException may throw an exception received from
     *         {@link MessageStream#onData(ByteBuffer, Object...)} calls
     * @return a stream to receive objects from the client
     */
    MessageStream clientConnection(Object object, MessageStream connection) throws ObjectCommunicationException;
}
