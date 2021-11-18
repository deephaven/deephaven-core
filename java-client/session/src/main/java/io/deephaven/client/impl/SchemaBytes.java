package io.deephaven.client.impl;

import io.deephaven.qst.table.TableHeader;

import java.util.Iterator;
import java.util.ServiceLoader;

public interface SchemaBytes {

    static byte[] of(TableHeader header) {
        final Iterator<SchemaBytes> it = ServiceLoader.load(SchemaBytes.class).iterator();
        if (!it.hasNext()) {
            throw new UnsupportedOperationException(String.format("Unable to find implementation for %s - " +
                    "likely need to include the 'deephaven-java-client-flight' jar.", SchemaBytes.class));
        }
        final SchemaBytes logic = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException(
                    String.format("Found at least 2 implementations for %s", SchemaBytes.class));
        }
        return logic.toSchemaBytes(header);
    }

    byte[] toSchemaBytes(TableHeader header);
}
