/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.PluginBase;

import java.io.IOException;
import java.io.OutputStream;

public abstract class ObjectTypeBase extends PluginBase implements ObjectType {
    public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        if (supportsBidiMessaging(object) == Kind.BIDIRECTIONAL) {
            // internal error, shouldn't have called this
            throw new IllegalStateException(
                    "Do not call writeTo if supportsBidiMessaging returns true, but writeTo is not implemented");
        } else {
            // incorrect implementation
            throw new IllegalStateException("ObjectType implementation returned false for supportsBidiMessaging");
        }
    }

    @Override
    public final void writeTo(Exporter exporter, Object object, OutputStream out) throws IOException {
        if (!isType(object)) {
            throw new IllegalArgumentException("Can't serialize object, wrong type: " + this + " / " + object);
        }
        writeCompatibleObjectTo(exporter, object, out);
    }

    @Override
    public final <T, V extends Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }
}
