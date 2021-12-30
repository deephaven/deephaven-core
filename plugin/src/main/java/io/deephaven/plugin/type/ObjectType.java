package io.deephaven.plugin.type;

import java.io.IOException;
import java.io.OutputStream;

public interface ObjectType {

    String name();

    boolean isType(Object o);

    void writeTo(Exporter exporter, Object o, OutputStream out) throws IOException;
}
