//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Table;
import io.deephaven.grpc.compression.CompressionCodecs;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Chooses the gRPC message encoding for a Barrage response from the table's {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE
 * allowed-encoding list} and the encodings the client advertised.
 */
public final class BarrageCompression {
    private static final Logger log = LoggerFactory.getLogger(BarrageCompression.class);

    private BarrageCompression() {}

    /**
     * Sets the response encoding of {@code responseObserver} to the first encoding in {@code attributes}' allowed list
     * that the client accepts. Leaves the response uncompressed when the table has no list, nothing matches, the list
     * is malformed, or the observer cannot be compressed. Must be called before the first response message is sent.
     *
     * @param responseObserver the raw gRPC response observer for the call
     * @param accepted the encodings the client advertised, from {@link ClientAcceptEncodingInterceptor}
     * @param attributes the attributes of the object being sent, or {@code null} if it has none
     * @param logName a description of the object being sent, for log messages
     * @return the chosen encoding, or empty if the response is not compressed
     */
    public static Optional<String> apply(
            @NotNull final StreamObserver<?> responseObserver,
            @NotNull final Set<String> accepted,
            @Nullable final AttributeMap<?> attributes,
            @NotNull final String logName) {
        if (attributes == null || accepted.isEmpty()) {
            return Optional.empty();
        }
        final Object value = attributes.getAttribute(Table.BARRAGE_COMPRESSION_ATTRIBUTE);
        if (value == null) {
            return Optional.empty();
        }

        final List<String> allowed;
        try {
            allowed = CompressionCodecs.parseList(value.toString());
        } catch (final IllegalArgumentException e) {
            log.warn().append("Ignoring ").append(Table.BARRAGE_COMPRESSION_ATTRIBUTE).append(" on ").append(logName)
                    .append(": ").append(e.getMessage()).endl();
            return Optional.empty();
        }

        final Optional<String> encoding = CompressionCodecs.choose(allowed, accepted);
        if (encoding.isEmpty()) {
            return encoding;
        }
        if (!(responseObserver instanceof ServerCallStreamObserver)) {
            return Optional.empty();
        }

        final ServerCallStreamObserver<?> call = (ServerCallStreamObserver<?>) responseObserver;
        try {
            // lock as GrpcUtil does for every other call on this observer
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (call) {
                call.setCompression(encoding.get());
            }
        } catch (final IllegalStateException e) {
            // headers were already sent; the response stays uncompressed
            log.warn().append("Unable to compress ").append(logName).append(" with ").append(encoding.get())
                    .append(": ").append(e.getMessage()).endl();
            return Optional.empty();
        }
        return encoding;
    }
}
