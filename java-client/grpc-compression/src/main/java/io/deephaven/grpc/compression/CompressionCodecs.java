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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * The gRPC message encodings Deephaven supports ({@code gzip}, {@code zstd} and {@code snappy}), their registries, and
 * the rule that picks a response encoding from a server-side allowed list and a client's {@code grpc-accept-encoding}.
 */
public final class CompressionCodecs {

    public static final String GZIP = "gzip";
    public static final String ZSTD = ZstdCodec.ENCODING;
    public static final String SNAPPY = SnappyCodec.ENCODING;

    /**
     * Every supported encoding name, excluding {@code identity}.
     */
    public static final List<String> SUPPORTED = List.of(GZIP, ZSTD, SNAPPY);

    private CompressionCodecs() {}

    /**
     * @return a registry holding a compressor for every supported encoding, plus {@code identity}
     */
    public static CompressorRegistry compressorRegistry() {
        final CompressorRegistry registry = CompressorRegistry.newEmptyInstance();
        registry.register(Codec.Identity.NONE);
        registry.register(new Codec.Gzip());
        registry.register(ZstdCodec.INSTANCE);
        registry.register(SnappyCodec.INSTANCE);
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
        final Set<String> advertise = new LinkedHashSet<>();
        for (final String name : advertised) {
            advertise.add(checkSupported(name));
        }
        return DecompressorRegistry.emptyInstance()
                .with(Codec.Identity.NONE, false)
                .with(new Codec.Gzip(), advertise.contains(GZIP))
                .with(ZstdCodec.INSTANCE, advertise.contains(ZSTD))
                .with(SnappyCodec.INSTANCE, advertise.contains(SNAPPY));
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
     * @return {@code name}, lower-cased
     * @throws IllegalArgumentException if {@code name} is not {@link #SUPPORTED supported}
     */
    public static String checkSupported(@NotNull final String name) {
        final String lower = name.trim().toLowerCase(Locale.ROOT);
        if (!SUPPORTED.contains(lower)) {
            throw new IllegalArgumentException(
                    "Unsupported compression '" + name + "'; supported values are " + String.join(", ", SUPPORTED));
        }
        return lower;
    }
}
