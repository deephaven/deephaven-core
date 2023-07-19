/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.PluginBase;

import java.io.IOException;
import java.io.OutputStream;

public abstract class ObjectTypeBase extends PluginBase implements ObjectType {
    public abstract void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException;

    @Override
    public final void writeTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        if (!isType(object)) {
            throw new IllegalArgumentException("Can't serialize object, wrong type: " + this + " / " + object);
        }
        writeCompatibleObjectTo(exporter, object, out);
    }

    @Override
    public boolean supportsBidiMessaging(Object obj) {
        return false;
    }

    @Override
    public void addMessageSender(Object object, MessageSender sender) {}

    @Override
    public void removeMessageSender() {}

    /**
     * Used by an ObjectType plugin to send a message to the client
     *
     * @param message The message to send to the client
     */
    @Override
    public void sendMessage(byte[] message) {}

    /**
     * Used by an ObjectType plugin to handle a message from the client
     *
     * @param message The message from the client
     */
    @Override
    public void handleMessage(byte[] message, Object object, Object[] referenceObjects) {}

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
