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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Chooses the compression type for a Barrage response from the table's {@link Table#TRANSPORT_COMPRESSION_ATTRIBUTE
 * allowed list}, or the server's {@link #DEFAULT_TYPES_PROPERTY default list}, and the compression types the client
 * accepts; and holds the compression types the server {@link #ENABLED_TYPES uses}.
 */
public final class TransportCompression {
    private static final Logger log = LoggerFactory.getLogger(TransportCompression.class);

    /**
     * The problems already logged, so that each is logged once rather than on every fetch: malformed lists, and the
     * names of compression types this server has no compressor for (the Netty server registers only {@code gzip}) or
     * that {@code TransportCompression.<type>.enabled} disables. The two never collide, because a single name always
     * parses. Declared before {@link #DEFAULT_ALLOWED}, whose initializer may add to it.
     */
    private static final Set<String> ALREADY_LOGGED_LIST = ConcurrentHashMap.newKeySet();

    /**
     * The compression types the server allows for a {@link Table} or {@link HierarchicalTable} that does not set
     * {@link Table#TRANSPORT_COMPRESSION_ATTRIBUTE}, as an ordered, comma-separated list in the attribute's format,
     * such as {@code zstd,snappy}. Unset by default, so such tables are never compressed. The server ignores a value it
     * cannot parse, and logs a warning.
     */
    public static final String DEFAULT_TYPES_PROPERTY = "TransportCompression.defaultTypes";

    private static final List<String> DEFAULT_ALLOWED =
            parseOrWarn(Configuration.getInstance().getStringWithDefault(DEFAULT_TYPES_PROPERTY, ""),
                    DEFAULT_TYPES_PROPERTY, List.of());

    /**
     * The compression types this server compresses responses and decompresses requests with, sorted by name: every
     * {@link CompressionCodecs#SUPPORTED supported} type whose {@code TransportCompression.<type>.enabled} property the
     * server's configuration does not set to {@code false}. The server's gRPC registries hold only these types.
     */
    public static final List<String> ENABLED_TYPES = enabledTypes(Configuration.getInstance());

    /**
     * The {@link CompressionCodecs#SUPPORTED supported} types that the server's configuration disables, sorted by name.
     * The server neither compresses nor decompresses with them, but tables may still name them, and the server then
     * uses the next type on the table's list.
     */
    public static final List<String> DISABLED_TYPES = CompressionCodecs.SUPPORTED.stream()
            .filter(type -> !ENABLED_TYPES.contains(type))
            .collect(Collectors.toUnmodifiableList());

    private TransportCompression() {}

    /**
     * Sets the compression of {@code responseObserver} to the first type in {@code attributes}' allowed list that the
     * client accepts and this server has a compressor for, using the server's {@link #DEFAULT_TYPES_PROPERTY default
     * list} when the object does not set one or sets one that cannot be parsed. Leaves the response uncompressed when
     * the object has no attributes, nothing matches, or the observer cannot be compressed. Must be called before the
     * first response message is sent.
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
        final Object value = attributes.getAttribute(Table.TRANSPORT_COMPRESSION_ATTRIBUTE);
        final List<String> allowed = value == null
                ? defaultAllowed
                : parseOrWarn(value.toString(), Table.TRANSPORT_COMPRESSION_ATTRIBUTE + " on " + logName,
                        defaultAllowed);

        if (!(responseObserver instanceof ServerCallStreamObserver)) {
            return Optional.empty();
        }

        final ServerCallStreamObserver<?> call = (ServerCallStreamObserver<?>) responseObserver;
        for (final String type : allowed) {
            if (!accepted.contains(type)) {
                continue;
            }
            if (DISABLED_TYPES.contains(type)) {
                if (ALREADY_LOGGED_LIST.add(type)) {
                    log.info().append("Not compressing ").append(logName).append(" with ").append(type)
                            .append(", disabled by ").append(enabledProperty(type)).endl();
                }
                continue;
            }
            try {
                // lock as GrpcUtil does for every other call on this observer
                // noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (call) {
                    call.setCompression(type);
                }
                return Optional.of(type);
            } catch (final IllegalArgumentException e) {
                // the server's compressor registry lacks this codec (the Netty server has only gzip); try the next type
                if (ALREADY_LOGGED_LIST.add(type)) {
                    log.warn().append("Unable to compress ").append(logName).append(" with ").append(type)
                            .append(": ").append(e.getMessage())
                            .append("; responses use the next allowed type instead, and this will not be logged again")
                            .endl();
                }
            } catch (final IllegalStateException e) {
                // headers were already sent, which no caller should allow; the response stays uncompressed
                log.warn().append("Unable to compress ").append(logName).append(" with ").append(type)
                        .append(": ").append(e.getMessage()).endl();
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * @param type the name of a compression type
     * @return the property that turns the type off on this server when set to {@code false}, in the server's
     *         configuration file or as a system property: {@code TransportCompression.<type>.enabled}
     */
    private static String enabledProperty(@NotNull final String type) {
        return "TransportCompression." + type + ".enabled";
    }

    /**
     * @return the {@link CompressionCodecs#SUPPORTED supported} types that {@code configuration} leaves enabled; logs
     *         each type that the configuration disables
     */
    static List<String> enabledTypes(@NotNull final Configuration configuration) {
        final List<String> enabled = new ArrayList<>();
        for (final String type : CompressionCodecs.SUPPORTED) {
            if (configuration.getBooleanWithDefault(enabledProperty(type), true)) {
                enabled.add(type);
            } else {
                log.info().append("Compression type '").append(type).append("' is disabled by ")
                        .append(enabledProperty(type)).endl();
            }
        }
        return Collections.unmodifiableList(enabled);
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
