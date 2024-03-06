//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import java.nio.ByteBuffer;
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
        final PyObject pyObject;
        if (object instanceof LivePyObjectWrapper) {
            pyObject = ((LivePyObjectWrapper) object).getPythonObject();
        } else if (object instanceof PyObject) {
            pyObject = (PyObject) object;
        } else {
            return false;
        }
        return objectTypeAdapter.call(boolean.class, "is_type", PyObject.class, pyObject);
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        final PyObject pyObject;
        if (object instanceof LivePyObjectWrapper) {
            pyObject = ((LivePyObjectWrapper) object).getPythonObject();
        } else if (object instanceof PyObject) {
            pyObject = (PyObject) object;
        } else {
            // This should be impossible, caught by the superclass's isType check
            throw new IllegalStateException(object + " is not a python object");
        }
        if (objectTypeAdapter.call("is_fetch_only").getBooleanValue()) {
            // Fall back and attempt to use old api:
            // Using this simple implementation, even though the python code won't write to this, but instead will
            // return a byte array directly
            Exporter exporter = new Exporter();

            final byte[] bytes = objectTypeAdapter.call(byte[].class, "to_bytes",
                    ExporterAdapter.class, new ExporterAdapter(exporter),
                    PyObject.class, pyObject);

            // Send the message and close the stream
            connection.onData(ByteBuffer.wrap(bytes), exporter.references());
            connection.onClose();

            return MessageStream.NOOP;
        } else {
            PyObject newConnection =
                    objectTypeAdapter.call(PyObject.class, "create_client_connection",
                            PyObject.class, pyObject,
                            PythonClientMessageStream.class, new PythonClientMessageStream(connection));
            return new PythonServerMessageStream(newConnection);
        }
    }

    private static class PythonClientMessageStream {
        private final MessageStream delegate;

        private PythonClientMessageStream(MessageStream delegate) {
            this.delegate = delegate;
        }

        @ScriptApi
        public void onData(byte[] payload, Object... references) throws ObjectCommunicationException {
            delegate.onData(ByteBuffer.wrap(payload), references);
        }

        @ScriptApi
        public void onClose() {
            delegate.onClose();
        }
    }

    private static class PythonServerMessageStream implements MessageStream {
        private final PyObject instance;

        private PythonServerMessageStream(PyObject instance) {
            this.instance = instance;
        }

        @Override
        public void onData(ByteBuffer payload, Object[] references) {
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);

            // Make a new array holding objects to pass to python, to avoid mutating the passed array. Note that this
            // will have no impact on GC/liveness, as the JVM is free to GC the original references array after the loop
            // but before the call() invocation.
            Object[] pyReferences = new Object[references.length];
            for (int i = 0; i < references.length; i++) {
                Object reference = references[i];
                if (reference instanceof LivePyObjectWrapper) {
                    reference = ((LivePyObjectWrapper) reference).getPythonObject();
                }
                pyReferences[i] = reference;
            }
            instance.call(void.class, "on_data", byte[].class, bytes, Object[].class, pyReferences);
        }

        @Override
        public void onClose() {
            instance.call("on_close");
            instance.close();
        }
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
