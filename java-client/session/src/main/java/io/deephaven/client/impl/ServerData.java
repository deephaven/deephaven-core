/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.Fetchable;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.proto.backplane.grpc.Data;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Data sent from the server to the client as part of a {@link Fetchable#fetch() fetch} or
 * {@link Bidirectional#connect(MessageStream) bidirectional message stream}. The client is responsible for
 * {@link ServerObject#close() closing} the {@link #exports()} when no longer needed.
 */
public final class ServerData implements Closeable {

    static ServerData of(Session session, Data data) {
        return new ServerData(data.getPayload(), toServerObjects(session, data.getExportedReferencesList()));
    }

    private static List<ServerObject> toServerObjects(Session session,
            List<io.deephaven.proto.backplane.grpc.TypedTicket> exportTickets) {
        return exportTickets
                .stream()
                .map(TypedTicket::from)
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
    private ServerData(ByteString data, List<ServerObject> exports) {
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
