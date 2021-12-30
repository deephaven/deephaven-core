package io.deephaven.client.impl;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public final class FetchedObject {
    private final String type;
    private final ByteString bytes;

    FetchedObject(String type, ByteString bytes) {
        this.type = Objects.requireNonNull(type);
        this.bytes = Objects.requireNonNull(bytes);
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

    @Override
    public String toString() {
        return "FetchedObject{" +
                "type='" + type + '\'' +
                ", bytes=" + bytes +
                '}';
    }
}
