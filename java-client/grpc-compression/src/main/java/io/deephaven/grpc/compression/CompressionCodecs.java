//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import io.grpc.Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The gRPC registries for every {@link CompressionType}, and the rule that picks a response encoding from a server-side
 * allowed list and a client's {@code grpc-accept-encoding}.
 */
public final class CompressionCodecs {

    /**
     * The {@link CompressionType#encodingName() encoding name} of every {@link CompressionType}, excluding
     * {@code identity}.
     */
    public static final List<String> SUPPORTED = Arrays.stream(CompressionType.values())
            .map(CompressionType::encodingName)
            .collect(Collectors.toUnmodifiableList());

    private CompressionCodecs() {}

    /**
     * @return a registry holding a compressor for every supported encoding, plus {@code identity}
     */
    public static CompressorRegistry compressorRegistry() {
        final CompressorRegistry registry = CompressorRegistry.newEmptyInstance();
        registry.register(Codec.Identity.NONE);
        for (final CompressionType type : CompressionType.values()) {
            registry.register(type.codec());
        }
        return registry;
    }

    /**
     * Builds a registry that can decode every supported encoding, but advertises only {@code advertised} in the
     * {@code grpc-accept-encoding} header.
     *
     * @param advertised the encodings to advertise; each must be {@link #SUPPORTED supported}
     * @return the registry
     */
    public static DecompressorRegistry decompressorRegistry(@NotNull final Collection<String> advertised) {
        final Set<CompressionType> advertise = EnumSet.noneOf(CompressionType.class);
        for (final String name : advertised) {
            advertise.add(CompressionType.fromEncodingName(name));
        }
        DecompressorRegistry registry = DecompressorRegistry.emptyInstance().with(Codec.Identity.NONE, false);
        for (final CompressionType type : CompressionType.values()) {
            registry = registry.with(type.codec(), advertise.contains(type));
        }
        return registry;
    }

    /**
     * Parses an ordered, comma-separated list of encoding names, such as {@code "zstd, snappy, gzip"}. Names are
     * trimmed and lower-cased, blank entries are skipped, and repeats keep their first position.
     *
     * @param list the list to parse; {@code null} or blank yields an empty list
     * @return the encodings, in order of preference
     * @throws IllegalArgumentException if a name is not {@link #SUPPORTED supported}
     */
    public static List<String> parseList(@Nullable final String list) {
        if (list == null || list.isBlank()) {
            return List.of();
        }
        final Set<String> names = new LinkedHashSet<>();
        for (final String part : list.split(",")) {
            final String name = part.trim().toLowerCase(Locale.ROOT);
            if (!name.isEmpty()) {
                names.add(checkSupported(name));
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(names));
    }

    /**
     * Parses the value of a {@code grpc-accept-encoding} header. Unlike {@link #parseList(String)}, unknown names are
     * kept; they simply never match.
     *
     * @param header the header value; {@code null} yields an empty set
     * @return the lower-cased encoding names
     */
    public static Set<String> parseAcceptEncoding(@Nullable final String header) {
        if (header == null || header.isBlank()) {
            return Set.of();
        }
        final Set<String> names = new LinkedHashSet<>();
        for (final String part : header.split(",")) {
            final String name = part.trim().toLowerCase(Locale.ROOT);
            if (!name.isEmpty()) {
                names.add(name);
            }
        }
        return Collections.unmodifiableSet(names);
    }

    /**
     * Picks the response encoding: the first entry of {@code allowed} that the client accepts.
     *
     * @param allowed the encodings the server allows, in order of preference
     * @param accepted the encodings the client advertised in {@code grpc-accept-encoding}
     * @return the chosen encoding, or empty to send uncompressed
     */
    public static Optional<String> choose(
            @NotNull final List<String> allowed,
            @NotNull final Set<String> accepted) {
        for (final String name : allowed) {
            if (accepted.contains(name)) {
                return Optional.of(name);
            }
        }
        return Optional.empty();
    }

    /**
     * @param name an encoding name
     * @return {@code name}, trimmed and lower-cased
     * @throws IllegalArgumentException if {@code name} is not {@link #SUPPORTED supported}
     */
    public static String checkSupported(@NotNull final String name) {
        return CompressionType.fromEncodingName(name).encodingName();
    }
}
