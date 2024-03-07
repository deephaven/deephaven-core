//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.type;

import io.deephaven.plugin.PluginBase;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * Abstract base class for object type plugins, providing some simple implementation details.
 */
public abstract class ObjectTypeBase extends PluginBase implements ObjectType {
    @Override
    public final MessageStream clientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        if (!isType(object)) {
            throw new IllegalArgumentException("Can't serialize object, wrong type: " + this + " / " + object);
        }
        return compatibleClientConnection(object, connection);
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
    public abstract MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException;

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }

    /**
     * Abstract base class for object type plugins that can only be fetched (and will not have later responses or accept
     * later requests). For bidirectional messages, see {@link ObjectTypeBase}.
     */
    public abstract static class FetchOnly extends ObjectTypeBase {

        /**
         * Serializes {@code object} as bytes to {@code out}. Must only be called with {@link #isType(Object) a
         * compatible object}.
         *
         * Server-side objects that should be sent as references to the client (but not themselves serialized in this
         * payload) can be exported using the {@code exporter} - each returned {@link Exporter.Reference} will have an
         * {@code index}, denoting its position on the array of exported objects to be received by the client.
         *
         * @param exporter the exporter
         * @param object the compatible object
         * @param out the output stream
         * @throws IOException if output stream operations failed, the export will then fail, and the client will get a
         *         generic error
         */
        public abstract void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out)
                throws IOException;

        @Override
        public MessageStream compatibleClientConnection(Object object, MessageStream connection)
                throws ObjectCommunicationException {
            Exporter exporter = new Exporter();

            try {
                writeCompatibleObjectTo(exporter, object, exporter.outputStream());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            connection.onData(exporter.payload(), exporter.references());
            connection.onClose();
            return MessageStream.NOOP;
        }
    }
}
