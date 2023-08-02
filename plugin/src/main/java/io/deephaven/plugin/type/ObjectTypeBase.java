/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.PluginBase;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Abstract base class for object type plugins, providing some simple implementation details.
 */
public abstract class ObjectTypeBase extends PluginBase implements ObjectType {

    public abstract static class FetchOnly extends ObjectTypeBase {

        public abstract void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out)
                throws IOException;

        @Override
        public MessageStream compatibleClientConnection(Object object, MessageStream connection) {
            StreamExporterImpl exporter = new StreamExporterImpl();

            try {
                writeCompatibleObjectTo(exporter, object, exporter.outputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            connection.onData(exporter.payload(), exporter.references());
            connection.onClose();
            return MessageStream.NOOP;
        }
    }

    @Override
    public final MessageStream clientConnection(Object object, MessageStream connection) {
        if (!isType(object)) {
            throw new IllegalArgumentException("Can't serialize object, wrong type: " + this + " / " + object);
        }
        return compatibleClientConnection(object, connection);
    }

    public abstract MessageStream compatibleClientConnection(Object object, MessageStream connection);

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }

}
