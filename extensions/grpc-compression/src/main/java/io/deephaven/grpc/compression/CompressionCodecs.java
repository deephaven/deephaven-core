//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import io.grpc.Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The compression types that Deephaven supports for gRPC messages, named as in gRPC's {@code grpc-encoding} and
 * {@code grpc-accept-encoding} headers; their gRPC registries; and parsers for compression type lists and a client's
 * {@code grpc-accept-encoding}. The supported types are the ones that the {@link CompressionCodecProvider}s on the
 * classpath supply: {@code zstd}, {@code gzip}, {@code snappy} and {@code deflate}, whose providers this module
 * includes, plus every type that a deployment's own provider adds.
 */
public final class CompressionCodecs {

    /**
     * Declared before {@link #CODECS}, whose initializer logs the providers it leaves out.
     */
    private static final Logger log = LoggerFactory.getLogger(CompressionCodecs.class);

    /**
     * The form of a compression type's name: lower-case, because names are matched lower-cased, and without the commas
     * and whitespace that separate names in lists and headers. Declared before {@link #CODECS}, whose initializer uses
     * it.
     */
    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9._+-]*");

    /**
     * Every supported codec, by its encoding name, sorted by name: the codec of each {@link CompressionCodecProvider}
     * on the classpath. The registries, {@link #SUPPORTED}, {@link #checkSupported(String)} and {@link #codec(String)}
     * all read it.
     */
    private static final Map<String, Codec> CODECS = byEncodingName(ServiceLoader.load(CompressionCodecProvider.class));

    /**
     * The name of every supported compression type, excluding {@code identity}, sorted by name. The providers this
     * module includes supply:
     * <ul>
     * <li>{@code deflate}: the algorithm of {@code gzip} in the zlib format, which compresses to the same size at the
     * same speed and decompresses slightly slower. Clients built on gRPC Core, such as the Python, C++ and R clients,
     * can decode it and {@code gzip}, but no other type this module includes.</li>
     * <li>{@code gzip}: compresses slightly smaller than {@code zstd} but much more slowly. Most gRPC clients can
     * decode it.</li>
     * <li>{@code snappy}: the fastest of these types, but the one that saves the least.</li>
     * <li>{@code zstd}: Zstandard, which compresses nearly as well as {@code gzip}, and many times faster.</li>
     * </ul>
     * Every other {@link CompressionCodecProvider} on the classpath adds its type, or replaces the built-in type of the
     * same name.
     */
    public static final List<String> SUPPORTED = List.copyOf(CODECS.keySet());

    private CompressionCodecs() {}

    /**
     * Collects the codec of each of {@code providers}. Failures are logged, never thrown, because this runs in the
     * static initializer: a provider that cannot be loaded, whose {@link CompressionCodecProvider#codec() codec()}
     * throws, or that supplies no codec or a codec whose name is malformed or {@code identity} is logged and left out,
     * and the other providers are still used. When two providers supply the same name, a deployment's provider replaces
     * a {@link #isBuiltIn built-in} one, and of two deployment providers the one whose class name sorts first is kept,
     * whatever the classpath order; the choice is logged.
     *
     * @param providers the providers of the codecs
     * @return the codec of each provider kept, by encoding name, sorted by name
     */
    static Map<String, Codec> byEncodingName(final Iterable<CompressionCodecProvider> providers) {
        final Map<String, Codec> byName = new TreeMap<>();
        // which provider supplied each name in byName, to settle a name that two providers supply
        final Map<String, CompressionCodecProvider> sources = new HashMap<>();
        final Iterator<CompressionCodecProvider> iterator = providers.iterator();
        while (true) {
            final CompressionCodecProvider provider;
            try {
                if (!iterator.hasNext()) {
                    break;
                }
                provider = iterator.next();
            } catch (final ServiceConfigurationError | LinkageError e) {
                // ServiceLoader throws from next() for a provider class that cannot be found or instantiated, and
                // from hasNext() for a provider class whose superclass or interface is missing; either way,
                // ServiceLoader moves on to the next provider
                log.warn("Ignoring a compression codec provider that could not be loaded", e);
                continue;
            }
            final String source = provider.getClass().getName();
            final Codec codec;
            try {
                codec = provider.codec();
            } catch (final Exception | LinkageError e) {
                // LinkageError covers a missing or broken native library and a failed static initializer
                log.warn("Ignoring compression codec provider {}, whose codec could not be created", source, e);
                continue;
            }
            if (codec == null) {
                log.warn("Ignoring compression codec provider {}, which supplies no codec", source);
                continue;
            }
            final String name = codec.getMessageEncoding();
            if (name == null || !NAME.matcher(name).matches()
                    || name.equals(Codec.Identity.NONE.getMessageEncoding())) {
                log.warn("Ignoring compression codec provider {}, which provides a codec named '{}'; a compression"
                        + " type name must be lower-case letters, digits, '.', '_', '+' and '-', starting with a"
                        + " letter or digit, and must not be 'identity'", source, name);
                continue;
            }
            final CompressionCodecProvider existing = sources.get(name);
            if (existing != null) {
                final boolean replace = shouldReplace(existing, provider);
                final CompressionCodecProvider kept = replace ? provider : existing;
                final CompressionCodecProvider ignored = replace ? existing : provider;
                if (isBuiltIn(ignored) && !isBuiltIn(kept)) {
                    log.info("Compression type '{}' from {} replaces the built-in type from {}",
                            name, kept.getClass().getName(), ignored.getClass().getName());
                } else {
                    log.warn("Compression type '{}' is provided by both {} and {}; using {}",
                            name, existing.getClass().getName(), source, kept.getClass().getName());
                }
                if (!replace) {
                    continue;
                }
            }
            sources.put(name, provider);
            byName.put(name, codec);
        }
        return Collections.unmodifiableMap(byName);
    }

    /**
     * @return whether {@code candidate} should replace {@code current} for a compression type both supply: a
     *         deployment's provider replaces a {@link #isBuiltIn built-in} one, and otherwise the provider whose class
     *         name sorts first is kept
     */
    private static boolean shouldReplace(
            @NotNull final CompressionCodecProvider current,
            @NotNull final CompressionCodecProvider candidate) {
        if (isBuiltIn(candidate) != isBuiltIn(current)) {
            return !isBuiltIn(candidate);
        }
        return candidate.getClass().getName().compareTo(current.getClass().getName()) < 0;
    }

    /**
     * @return whether {@code provider} is one of the providers this module includes, which a deployment's provider of
     *         the same compression type replaces
     */
    private static boolean isBuiltIn(@NotNull final CompressionCodecProvider provider) {
        return provider instanceof DeephavenCompressionCodecProvider;
    }

    /**
     * @return a registry holding a compressor for every supported encoding, plus {@code identity}
     */
    public static CompressorRegistry compressorRegistry() {
        return compressorRegistry(SUPPORTED);
    }

    /**
     * @param types the encodings to register; each must be {@link #SUPPORTED supported}
     * @return a registry holding a compressor for each of {@code types}, plus {@code identity}
     */
    public static CompressorRegistry compressorRegistry(@NotNull final Collection<String> types) {
        final CompressorRegistry registry = CompressorRegistry.newEmptyInstance();
        registry.register(Codec.Identity.NONE);
        for (final String type : types) {
            registry.register(codec(type));
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
        return decompressorRegistry(SUPPORTED, advertised);
    }

    /**
     * Builds a registry that can decode each of {@code types}, but advertises only those also in {@code advertised} in
     * the {@code grpc-accept-encoding} header.
     *
     * @param types the encodings to decode; each must be {@link #SUPPORTED supported}
     * @param advertised the encodings to advertise; each must be {@link #SUPPORTED supported}
     * @return the registry
     */
    public static DecompressorRegistry decompressorRegistry(
            @NotNull final Collection<String> types,
            @NotNull final Collection<String> advertised) {
        final Set<String> advertise = new HashSet<>();
        for (final String name : advertised) {
            advertise.add(checkSupported(name));
        }
        DecompressorRegistry registry = DecompressorRegistry.emptyInstance().with(Codec.Identity.NONE, false);
        for (final String type : types) {
            final String name = checkSupported(type);
            registry = registry.with(CODECS.get(name), advertise.contains(name));
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
