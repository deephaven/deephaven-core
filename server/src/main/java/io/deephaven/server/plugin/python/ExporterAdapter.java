package io.deephaven.server.plugin.python;

import io.deephaven.plugin.type.ObjectType.Exporter;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;
import org.jpy.PyObject;

import java.util.Objects;

final class ExporterAdapter {

    private final Exporter exporter;

    public ExporterAdapter(Exporter exporter) {
        this.exporter = Objects.requireNonNull(exporter);
    }

    public Reference newReference(Object object) {
        return exporter.newReference(object);
    }

    // TODO(deephaven-core#1775): multivariate jpy (unwrapped) call into java
    public Reference newReferencePyObject(PyObject object) {
        return exporter.newReference(object);
    }

    public Reference reference(Object object) {
        return exporter.reference(object);
    }

    // TODO(deephaven-core#1775): multivariate jpy (unwrapped) call into java
    public Reference referencePyObject(PyObject object) {
        return exporter.reference(object, Objects::equals);
    }
}
