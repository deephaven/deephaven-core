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

    public Reference newServerSideReference(Object object) {
        return exporter.newServerSideReference(object);
    }

    // TODO(deephaven-core#1775): multivariate jpy (unwrapped) call into java
    public Reference newServerSideReferencePyObject(PyObject object) {
        return exporter.newServerSideReference(object);
    }
}
