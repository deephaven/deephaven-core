/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The results from fetching an object.
 *
 * @see Session#fetchObject(HasTypedTicket)
 */
public final class FetchedObject implements HasTypedTicket, Closeable {
    private final TypedTicket typedTicket;
    private final ByteString bytes;
    private final List<ServerObject> exports;

    /**
     * Constructs a new instance. Callers should not modify {@code exports} after construction.
     *
     * @param typedTicket the typed ticket
     * @param bytes the bytes
     * @param exports the exports
     */
    FetchedObject(TypedTicket typedTicket, ByteString bytes, List<ServerObject> exports) {
        this.typedTicket = Objects.requireNonNull(typedTicket);
        this.bytes = Objects.requireNonNull(bytes);
        this.exports = Collections.unmodifiableList(exports);
        if (!typedTicket.type().isPresent()) {
            throw new IllegalArgumentException("Must only construct fetched object with known type");
        }
    }

    public String type() {
        return typedTicket.type().orElseThrow(IllegalStateException::new);
    }

    @Override
    public TicketId ticketId() {
        return typedTicket.ticketId();
    }

    @Override
    public TypedTicket typedTicket() {
        return typedTicket;
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

    /**
     * The export ids.
     *
     * @return the export ids
     * @deprecated use {@link #exports()}
     */
    @Deprecated
    public List<ExportId> exportIds() {
        return exports.stream().map(HasExportId::exportId).collect(Collectors.toList());
    }

    /**
     * The exported server objects.
     *
     * @return the exported server objects
     */
    public List<ServerObject> exports() {
        return exports;
    }

    @Override
    public String toString() {
        return "FetchedObject{" +
                "typedTicket='" + typedTicket + '\'' +
                ", bytes=" + bytes +
                ", exports=" + exports +
                '}';
    }

    /**
     * Closes all of {@link #exports}.
     */
    @Override
    public void close() {
        for (ServerObject export : exports) {
            export.close();
        }
    }
}
