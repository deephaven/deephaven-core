/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * An implementation that uses strict {@link Class} equality for the {@link #isType(Object)} check.
 * 
 * @param <T> the class type
 */
public abstract class ObjectTypeClassBase<T> extends ObjectTypeBase {
    private final String name;
    private final Class<T> clazz;

    public ObjectTypeClassBase(String name, Class<T> clazz) {
        this.name = Objects.requireNonNull(name);
        this.clazz = Objects.requireNonNull(clazz);
    }

    public final Class<T> clazz() {
        return clazz;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final boolean isType(Object object) {
        return clazz.equals(object.getClass());
    }

    @Override
    public final MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        return clientConnectionImpl((T) object, connection);
    }

    public abstract MessageStream clientConnectionImpl(T object, MessageStream connection)
            throws ObjectCommunicationException;

    @Override
    public String toString() {
        return name + ":" + clazz.getName();
    }

    /**
     * Abstract base class for object type plugins that can only be fetched (and will not have later responses or accept
     * later requests). For bidirectional messages, see {@link ObjectTypeClassBase}.
     *
     * @param <T> the class type
     */
    public abstract static class FetchOnly<T> extends ObjectTypeClassBase<T> {

        public FetchOnly(String name, Class<T> clazz) {
            super(name, clazz);
        }

        @Override
        public final MessageStream clientConnectionImpl(T object, MessageStream connection)
                throws ObjectCommunicationException {
            Exporter exporter = new Exporter();

            try {
                writeToImpl(exporter, object, exporter.outputStream());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            connection.onData(exporter.payload(), exporter.references());
            connection.onClose();

            return MessageStream.NOOP;
        }

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
         *
         * @throws IOException if output stream operations failed, the export will then fail, and the client will get a
         *         generic error
         */
        public abstract void writeToImpl(Exporter exporter, T object, OutputStream out) throws IOException;
    }

}
