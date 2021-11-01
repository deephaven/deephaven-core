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
    public String toString() {
        return objectTypeAdapter.toString();
    }

    @Override
    public void close() {
        objectTypeAdapter.close();
    }
}
