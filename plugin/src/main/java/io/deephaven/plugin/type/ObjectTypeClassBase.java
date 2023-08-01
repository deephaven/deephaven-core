/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An implementation that uses strict {@link Class} equality for the {@link #isType(Object)} check.
 * 
 * @param <T> the class type
 */
public abstract class ObjectTypeClassBase<T> extends ObjectTypeBase {
    public abstract static class FetchOnly<T> extends ObjectTypeClassBase<T> {

        public FetchOnly(String name, Class<T> clazz) {
            super(name, clazz);
        }

        @Override
        public final MessageStream clientConnectionImpl(T object, MessageStream connection) {
            StreamExporterImpl exporter = new StreamExporterImpl();

            try {
                writeToImpl(exporter, object, exporter.outputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            connection.onMessage(exporter.payload(), exporter.references());
            connection.close();

            return MessageStream.NOOP;
        }

        public abstract void writeToImpl(Exporter exporter, T object, OutputStream out) throws IOException;
    }

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
    public final MessageStream compatibleClientConnection(Object object, MessageStream connection) {
        return clientConnectionImpl((T) object, connection);
    }

    public abstract MessageStream clientConnectionImpl(T object, MessageStream connection);

    @Override
    public String toString() {
        return name + ":" + clazz.getName();
    }
}
