/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.BiPredicate;

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
            public void onData(ByteBuffer payload, Object[] references) {}

            @Override
            public void onClose() {}
        };

        /**
         * Transmits data to the remote end of the stream. This can consist of a binary payload and references to
         * objects on the server.
         *
         * @param payload the binary data sent to the remote implementation
         * @param references server-side object references sent to the remote implementation
         */
        void onData(ByteBuffer payload, Object[] references);

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
     * @return a stream to receive objects from the client
     */
    MessageStream clientConnection(Object object, MessageStream connection);

    interface Exporter {

        default Reference reference(Object object) {
            // noinspection OptionalGetWithoutIsPresent
            return reference(object, true, true).get();
        }

        /**
         * Gets the reference for {@code object} if it has already been created and {@code forceNew} is {@code false},
         * otherwise creates a new one. If {@code allowUnknownType} is {@code false}, and no type can be found, no
         * reference will be created. Uses reference-based equality.
         *
         * @param object the object
         * @param allowUnknownType if an unknown-typed reference can be created
         * @param forceNew if a new reference should be created
         * @return the reference
         */
        @Deprecated
        Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew);

        /**
         * Gets the reference for {@code object} if it has already been created and {@code forceNew} is {@code false},
         * otherwise creates a new one. If {@code allowUnknownType} is {@code false}, and no type can be found, no
         * reference will be created.
         *
         * @param object the object
         * @param allowUnknownType if an unknown-typed reference can be created
         * @param forceNew if a new reference should be created
         * @param equals the equals logic
         * @return the reference
         */
        @Deprecated
        Optional<Reference> reference(Object object, boolean allowUnknownType, boolean forceNew,
                BiPredicate<Object, Object> equals);

        /**
         * A reference.
         */
        interface Reference {
            /**
             * The index, which is defined by the order in which references are created. May be used in the output
             * stream to refer to the reference from the client.
             *
             * @return the index
             */
            int index();

            /**
             * The type.
             *
             * @deprecated As of 0.27.0, this will always return empty.
             * @return the type, if present
             */
            @Deprecated
            default Optional<String> type() {
                return Optional.empty();
            }
        }
    }
}
