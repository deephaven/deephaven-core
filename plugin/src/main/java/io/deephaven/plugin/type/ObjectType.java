/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;

import java.io.IOException;
import java.io.OutputStream;
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
     * Serializes {@code object} into {@code out}. Must only be called with a compatible object, see
     * {@link #isType(Object)}.
     *
     * <p>
     * Objects that {@code object} references may be serialized as {@link Reference}.
     *
     * <p>
     * Note: the implementation should not hold onto references nor create references outside the calling thread.
     *
     * @param exporter the exporter
     * @param object the (compatible) object
     * @param out the output stream
     * @throws IOException if an IO exception occurs
     */
    default void writeTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        if (supportsBidiMessaging(object) == Kind.BIDIRECTIONAL) {
            // internal error, shouldn't have called this
            throw new IllegalStateException(
                    "Do not call writeTo if supportsBidiMessaging returns true, but writeTo is not implemented");
        } else {
            // incorrect implementation
            throw new IllegalStateException("ObjectType implementation returned false for supportsBidiMessaging");
        }
    }

    /**
     * A stream of messages, either sent from the server to the client, or client to the server. ObjectType plugin
     * implementations can provide an implementation of this interface for each incoming stream to invoke as messages
     * arrive, and will likewise be given an instance of this interface to be able to send messages to the client.
     */
    interface MessageStream extends AutoCloseable {
        /**
         * Transmits data to the remote end of the stream. This can consist of a binary payload and references to
         * objects on the server.
         *
         * @param payload the binary data sent to the remote implementation
         * @param references server-side object references sent to the remote implementation
         */
        void onMessage(ByteBuffer payload, Object[] references);

        /**
         * Closes the stream on both ends. No further messages can be sent or received.
         */
        void close();
    }

    /**
     * Signals creation of a client stream to the provided object. The returned MessageStream implementation will be
     * called with each received message from the server, and can call the provided connection instance to send messages
     * as needed to the client.
     * 
     * @param object the object to create a connection for
     * @param connection a stream to send objects to the client
     * @return a stream to receive objects from the client
     */
    // impl note: provide default impl? deprecate writeTo?
    default MessageStream clientConnection(Object object, MessageStream connection) {
        if (supportsBidiMessaging(object) == Kind.BIDIRECTIONAL) {
            // incorrect implementation
            throw new IllegalStateException(
                    "ObjectType implementation returned true for supportsBidiMessaging, but has no clientConnection implementation");
        } else {
            // internal error, shouldn't have called this
            throw new IllegalStateException("Do not call clientConnection if supportsBidiMessaging returns false");
        }
    }

    /**
     * Consider renaming to API? MessageAPI?
     */
    enum Kind {
        /**
         * An object can be fetched, but will not continue to stream results, or receive messages from the server.
         */
        FETCHABLE,
        /**
         * An object can be connected to by a client, and continue to stream messages server to client or client to
         * server.
         */
        BIDIRECTIONAL,
    }

    /**
     * Returns true if the {@code object} supports bidirectional communication.
     *
     * @param object the object
     * @return true if the {@code object} supports bidirectional communication
     */
    // TODO review: should the objecttype itself just return fetchable vs bidi? or do we actually want to decide on an
    // object-by-object basis?
    default Kind supportsBidiMessaging(Object object) {
        return Kind.FETCHABLE;
    }

    /**
     * The interface for creating new references during the {@link #writeTo(Exporter, Object, OutputStream)}.
     */
    interface Exporter {

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
             * @return the type, if present
             */
            Optional<String> type();
        }
    }
}
