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
     *
     */
    interface MessageStream extends AutoCloseable {
        /**
         *
         * @param message
         * @param references
         */
        void onMessage(ByteBuffer message, Object[] references);

        /**
         *
         */
        void close();
    }

    /**
     * Signals creation of a client stream to the provided object. The returned MessageStream implementation will be
     * called with each received message from the server, and can call the provided connection instance to send messages
     * as needed to the client.
     * 
     * @param object
     * @param connection
     * @return
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

    enum Kind {
        FETCHABLE, BIDIRECTIONAL,
    }

    /**
     * Returns true if the {@code object} supports bidirectional communication.
     *
     * @param object the object
     * @return true if the {@code object} supports bidirectional communication
     */
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
