//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.AttributeMap;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Chooses the compression type for a Barrage response from the table's {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE
 * allowed list}, or the server's {@link #DEFAULT_TYPES_PROPERTY default list}, and the compression types the client
 * accepts.
 */
public final class BarrageCompression {
    private static final Logger log = LoggerFactory.getLogger(BarrageCompression.class);

    /**
     * The malformed lists already logged, so that a bad value is logged once rather than on every fetch of every table
     * that carries it. Declared before {@link #DEFAULT_ALLOWED}, whose initializer may add to it.
     */
    private static final Set<String> ALREADY_LOGGED_LIST = ConcurrentHashMap.newKeySet();

    /**
     * The compression types the server allows for a {@link Table} or {@link HierarchicalTable} that does not set
     * {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE}, as an ordered, comma-separated list in the attribute's format, such
     * as {@code zstd,snappy}. Unset by default, so such tables are never compressed. The server ignores a value it
     * cannot parse, and logs a warning.
     */
    public static final String DEFAULT_TYPES_PROPERTY = "BarrageCompression.defaultTypes";

    private static final List<String> DEFAULT_ALLOWED =
            parseOrWarn(Configuration.getInstance().getStringWithDefault(DEFAULT_TYPES_PROPERTY, ""),
                    DEFAULT_TYPES_PROPERTY, List.of());

    private BarrageCompression() {}

    /**
     * Sets the compression of {@code responseObserver} to the first type in {@code attributes}' allowed list that the
     * client accepts, using the server's {@link #DEFAULT_TYPES_PROPERTY default list} when the object does not set one
     * or sets one that cannot be parsed. Leaves the response uncompressed when the object has no attributes, nothing
     * matches, or the observer cannot be compressed. Must be called before the first response message is sent.
     *
     * @param responseObserver the raw gRPC response observer for the call
     * @param accepted the compression types the client accepts, from {@link ClientAcceptCompressionInterceptor}
     * @param attributes the attributes of the object being sent, or {@code null} if it has none
     * @param logName a description of the object being sent, for log messages
     * @return the chosen compression type, or empty if the response is not compressed
     */
    public static Optional<String> apply(
            @NotNull final StreamObserver<?> responseObserver,
            @NotNull final Set<String> accepted,
            @Nullable final AttributeMap<?> attributes,
            @NotNull final String logName) {
        return apply(responseObserver, accepted, attributes, logName, DEFAULT_ALLOWED);
    }

    /**
     * Like {@link #apply(StreamObserver, Set, AttributeMap, String)}, with the default list given rather than
     * configured.
     *
     * @param defaultAllowed the compression types to allow when {@code attributes} does not set a valid list
     */
    static Optional<String> apply(
            @NotNull final StreamObserver<?> responseObserver,
            @NotNull final Set<String> accepted,
            @Nullable final AttributeMap<?> attributes,
            @NotNull final String logName,
            @NotNull final List<String> defaultAllowed) {
        if (attributes == null) {
            return Optional.empty();
        }
        final Object value = attributes.getAttribute(Table.BARRAGE_COMPRESSION_ATTRIBUTE);
        final List<String> allowed = value == null
                ? defaultAllowed
                : parseOrWarn(value.toString(), Table.BARRAGE_COMPRESSION_ATTRIBUTE + " on " + logName,
                        defaultAllowed);

        final Optional<String> chosen = CompressionCodecs.choose(allowed, accepted);
        if (chosen.isEmpty()) {
            return chosen;
        }
        if (!(responseObserver instanceof ServerCallStreamObserver)) {
            return Optional.empty();
        }

        final ServerCallStreamObserver<?> call = (ServerCallStreamObserver<?>) responseObserver;
        try {
            // lock as GrpcUtil does for every other call on this observer
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (call) {
                call.setCompression(chosen.get());
            }
        } catch (final IllegalStateException | IllegalArgumentException e) {
            // headers were already sent, or the server's compressor registry lacks this codec; the response stays
            // uncompressed
            log.warn().append("Unable to compress ").append(logName).append(" with ").append(chosen.get())
                    .append(": ").append(e.getMessage()).endl();
            return Optional.empty();
        }
        return chosen;
    }

    /**
     * @return {@code list} parsed with {@link CompressionCodecs#parseList(String)}, or {@code fallback} if it cannot be
     *         parsed; the first time a given {@code list} fails, logs a warning that names {@code description}
     */
    private static List<String> parseOrWarn(
            @NotNull final String list,
            @NotNull final String description,
            @NotNull final List<String> fallback) {
        try {
            return CompressionCodecs.parseList(list);
        } catch (final IllegalArgumentException e) {
            if (ALREADY_LOGGED_LIST.add(list)) {
                log.warn().append("Ignoring ").append(description).append(": ").append(e.getMessage())
                        .append("; this value will not be logged again").endl();
            }
            return fallback;
        }
    }
}
