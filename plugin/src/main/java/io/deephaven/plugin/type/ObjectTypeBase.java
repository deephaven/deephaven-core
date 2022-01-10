package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;

public abstract class ObjectTypeBase implements ObjectType {

    public abstract void writeToCompatibleObject(Exporter exporter, Object object, OutputStream out) throws IOException;

    @Override
    public final void writeTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        if (!isType(object)) {
            throw new IllegalArgumentException("Can't serialize object, wrong type: " + this + " / " + object);
        }
        writeToCompatibleObject(exporter, object, out);
    }

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
