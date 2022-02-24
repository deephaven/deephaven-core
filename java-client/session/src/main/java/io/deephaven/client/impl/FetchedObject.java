package io.deephaven.client.impl;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class FetchedObject {
    private final String type;
    private final ByteString bytes;
    private final List<ExportId> exportIds;

    /**
     * Constructs a new instance. Callers should not modify {@code exportIds} after construction.
     *
     * @param type the type
     * @param bytes the bytes
     * @param exportIds the export ids
     */
    FetchedObject(String type, ByteString bytes, List<ExportId> exportIds) {
        this.type = Objects.requireNonNull(type);
        this.bytes = Objects.requireNonNull(bytes);
        this.exportIds = Collections.unmodifiableList(exportIds);
    }

    public String type() {
        return type;
    }

    public byte[] toByteArray() {
        return bytes.toByteArray();
    }

    public int size() {
        return bytes.size();
    }

    public void writeTo(OutputStream out) throws IOException {
        bytes.writeTo(out);
    }

    public List<ExportId> exportIds() {
        return exportIds;
    }

    @Override
    public String toString() {
        return "FetchedObject{" +
                "type='" + type + '\'' +
                ", bytes=" + bytes +
                ", exportIds=" + exportIds +
                '}';
    }
}
