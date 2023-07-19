/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.type.ObjectTypeBase;
import org.jpy.PyObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

final class ObjectTypeAdapter extends ObjectTypeBase implements AutoCloseable {

    private final String name;
    private final PyObject objectTypeAdapter;

    public ObjectTypeAdapter(String name, PyObject objectTypeAdapter) {
        this.name = Objects.requireNonNull(name);
        this.objectTypeAdapter = Objects.requireNonNull(objectTypeAdapter);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isType(Object object) {
        if (!(object instanceof PyObject)) {
            return false;
        }
        return objectTypeAdapter.call(boolean.class, "is_type", PyObject.class, (PyObject) object);
    }

    @Override
    public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        final byte[] bytes = objectTypeAdapter.call(byte[].class, "to_bytes",
                ExporterAdapter.class, new ExporterAdapter(exporter),
                PyObject.class, (PyObject) object);
        out.write(bytes);
    }

    @Override
    public boolean supportsBidiMessaging(Object object) {
        return objectTypeAdapter.call(boolean.class, "supports_bidi_messaging", PyObject.class, (PyObject) object);
    }

    @Override
    public void handleMessage(byte[] msg, Object object, Object[] referenceObjects) {
        objectTypeAdapter.call("handle_message", msg, object, referenceObjects);
    }

    @Override
    public void addMessageSender(Object object, MessageSender sender) {
        objectTypeAdapter.call("add_message_sender", object, sender);
    }

    @Override
    public void removeMessageSender() {
        objectTypeAdapter.call("remove_message_sender");
    }

    @Override
    public String toString() {
        return objectTypeAdapter.toString();
    }

    @Override
    public void close() {
        objectTypeAdapter.close();
    }
}
