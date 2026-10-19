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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The compression types that Deephaven supports for gRPC messages, named as in gRPC's {@code grpc-encoding} and
 * {@code grpc-accept-encoding} headers; their gRPC registries; and parsers for compression type lists and a client's
 * {@code grpc-accept-encoding}. The supported types are a built-in baseline, {@code zstd}, {@code gzip} and
 * {@code snappy}, plus every type that a {@link CompressionCodecProvider} on the classpath adds.
 */
public final class CompressionCodecs {

    /**
     * The form of a provided compression type's name: lower-case, because names are matched lower-cased, and without
     * the commas and whitespace that separate names in lists and headers. Declared before {@link #CODECS}, whose
     * initializer uses it.
     */
    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9._+-]*");

    /**
     * The baseline: the codecs that Deephaven includes, which {@link CompressionCodecProvider}s add to. Package-private
     * for tests of {@link #byEncodingName(List, Iterable)}.
     */
    static final List<Codec> BUILT_IN = List.of(ZstdCodec.INSTANCE, new Codec.Gzip(), SnappyCodec.INSTANCE);

    /**
     * Every supported codec, by its encoding name, in the order of {@link #SUPPORTED}: the {@link #BUILT_IN} baseline,
     * then the codec of each {@link CompressionCodecProvider} on the classpath. The registries, {@link #SUPPORTED},
     * {@link #checkSupported(String)} and {@link #codec(String)} all read it.
     */
    private static final Map<String, Codec> CODECS =
            byEncodingName(BUILT_IN, ServiceLoader.load(CompressionCodecProvider.class));

    /**
     * The name of every supported compression type, excluding {@code identity}: the built-in baseline, then the types
     * that {@link CompressionCodecProvider}s on the classpath add. The baseline is:
     * <ul>
     * <li>{@code zstd}: Zstandard, which compresses nearly as well as {@code gzip}, and many times faster.</li>
     * <li>{@code gzip}: compresses slightly smaller than {@code zstd} but much more slowly. Most gRPC clients can
     * decode it.</li>
     * <li>{@code snappy}: the fastest of the built-in types, but the one that saves the least.</li>
     * </ul>
     */
    public static final List<String> SUPPORTED = List.copyOf(CODECS.keySet());

    private CompressionCodecs() {}

    /**
     * @param builtIns the codecs that Deephaven includes
     * @param providers the providers of more codecs
     * @return every codec, by encoding name: {@code builtIns} in order, then the codec of each of {@code providers}
     * @throws IllegalStateException if a provider supplies no codec, or one whose name is malformed, {@code identity},
     *         or the name of another codec
     */
    static Map<String, Codec> byEncodingName(
            final List<Codec> builtIns,
            final Iterable<CompressionCodecProvider> providers) {
        final Map<String, Codec> byName = new LinkedHashMap<>();
        // where each name came from, for errors about a repeated name
        final Map<String, String> sources = new HashMap<>();
        for (final Codec codec : builtIns) {
            byName.put(codec.getMessageEncoding(), codec);
            sources.put(codec.getMessageEncoding(), "Deephaven");
        }
        for (final CompressionCodecProvider provider : providers) {
            final String source = provider.getClass().getName();
            final Codec codec = provider.codec();
            if (codec == null) {
                throw new IllegalStateException(source + " provides no codec");
            }
            final String name = codec.getMessageEncoding();
            if (name == null || !NAME.matcher(name).matches()
                    || name.equals(Codec.Identity.NONE.getMessageEncoding())) {
                throw new IllegalStateException(source + " provides a codec named '" + name
                        + "'; a compression type name must be lower-case letters, digits, '.', '_', '+' and '-',"
                        + " starting with a letter or digit, and must not be 'identity'");
            }
            final String existing = sources.putIfAbsent(name, source);
            if (existing != null) {
                throw new IllegalStateException(
                        "Compression type '" + name + "' is provided by both " + existing + " and " + source);
            }
            byName.put(name, codec);
        }
        return Collections.unmodifiableMap(byName);
    }

    /**
     * @return a registry holding a compressor for every supported encoding, plus {@code identity}
     */
    public static CompressorRegistry compressorRegistry() {
        final CompressorRegistry registry = CompressorRegistry.newEmptyInstance();
        registry.register(Codec.Identity.NONE);
        for (final Codec codec : CODECS.values()) {
            registry.register(codec);
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
        final Set<String> advertise = new HashSet<>();
        for (final String name : advertised) {
            advertise.add(checkSupported(name));
        }
        DecompressorRegistry registry = DecompressorRegistry.emptyInstance().with(Codec.Identity.NONE, false);
        for (final Map.Entry<String, Codec> entry : CODECS.entrySet()) {
            registry = registry.with(entry.getValue(), advertise.contains(entry.getKey()));
        }
        return registry;
    }

    /**
     * @param name the name of a compression type; case and surrounding whitespace are ignored
     * @return the codec for {@code name}, the one that {@link #compressorRegistry()} and
     *         {@link #decompressorRegistry(Collection)} register
     * @throws IllegalArgumentException if {@code name} is not {@link #SUPPORTED supported}
     */
    public static Codec codec(@NotNull final String name) {
        return CODECS.get(checkSupported(name));
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
     * @param name the name of a compression type; case and surrounding whitespace are ignored
     * @return {@code name}, trimmed and lower-cased
     * @throws IllegalArgumentException if {@code name} is not {@link #SUPPORTED supported}
     */
    public static String checkSupported(@NotNull final String name) {
        final String lower = name.trim().toLowerCase(Locale.ROOT);
        if (!CODECS.containsKey(lower)) {
            throw new IllegalArgumentException("Unsupported compression '" + name + "'; supported values are "
                    + String.join(", ", SUPPORTED));
        }
        return lower;
    }
}
