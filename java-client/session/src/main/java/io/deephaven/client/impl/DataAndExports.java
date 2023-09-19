/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.ServerObject.Bidirectional;
import io.deephaven.client.impl.ServerObject.Fetchable;
import io.deephaven.proto.backplane.grpc.Data;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Data and exports are sent from the server to the client as part of a {@link Fetchable#fetch() fetch} or
 * {@link Bidirectional#messageStream(MessageStream) bidirectional message stream}. The client is responsible for
 * managing the {@link #exports()}.
 */
public final class DataAndExports implements Closeable {

    static DataAndExports of(Session session, Data data) {
        return new DataAndExports(data.getPayload(), toServerObjects(session, data.getExportedReferencesList()));
    }

    static DataAndExports of(Session session, FetchObjectResponse fetch) {
        return new DataAndExports(fetch.getData(), toServerObjects(session, fetch.getTypedExportIdsList()));
    }

    private static List<ServerObject> toServerObjects(Session session,
            List<io.deephaven.proto.backplane.grpc.TypedTicket> exportTickets) {
        return exportTickets
                .stream()
                .map(TypedTicket::of)
                .map(TypedTicket::toExportId)
                .map(exportId -> exportId.toServerObject(session))
                .collect(Collectors.toList());
    }

    private final ByteString data;
    private final List<ServerObject> exports;

    /**
     * Constructs a new instance. Callers should not modify {@code exports} after construction.
     *
     * @param data the bytes
     * @param exports the exports
     */
    DataAndExports(ByteString data, List<ServerObject> exports) {
        this.data = Objects.requireNonNull(data);
        this.exports = Collections.unmodifiableList(exports);
    }

    /**
     * The read-only data payload bytes. May be empty.
     *
     * @return the data
     */
    public ByteBuffer data() {
        return data.asReadOnlyByteBuffer();
    }

    /**
     * The exported server objects, may be empty.
     *
     * @return the exported server objects
     */
    public List<ServerObject> exports() {
        return exports;
    }

    /**
     * {@link ServerObject#release() Releases} all of the {@link #exports()}, returning the
     * {@link CompletableFuture#allOf(CompletableFuture[])}.
     *
     * @return the future
     */
    public CompletableFuture<Void> releaseExports() {
        return CompletableFuture.allOf(exports.stream().map(ServerObject::release).toArray(CompletableFuture[]::new));
    }

    /**
     * Closes all of the {@link #exports()} and {@code this}. For more control, callers may call
     * {@link #releaseExports()} or manage {@link #exports()} directly.
     */
    @Override
    public void close() {
        for (ServerObject export : exports) {
            export.close();
        }
    }
}
