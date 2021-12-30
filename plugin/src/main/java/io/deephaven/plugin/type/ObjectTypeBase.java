package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;

public abstract class ObjectTypeBase implements ObjectType {

    public abstract void writeToTypeChecked(Exporter exporter, Object object, OutputStream out) throws IOException;

    @Override
    public final void writeTo(Exporter exporter, Object o, OutputStream out) throws IOException {
        if (!isType(o)) {
            throw new IllegalArgumentException("Can't serialize object, wrong type: " + this + " / " + o);
        }
        writeToTypeChecked(exporter, o, out);
    }
}
