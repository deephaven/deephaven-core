//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import io.grpc.Codec;
import io.grpc.Compressor;
import io.grpc.CompressorRegistry;
import io.grpc.Decompressor;
import io.grpc.DecompressorRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionCodecsTest {

    @Test
    public void supportedNames() {
        // the providers this module includes; no other CompressionCodecProvider is on its test classpath
        assertThat(CompressionCodecs.SUPPORTED).containsExactly("deflate", "gzip", "snappy", "zstd");
    }

    @Test
    public void includedProvidersAreServices() {
        // the four are found like any deployment's provider, so a missing service entry would drop a type
        assertThat(ServiceLoader.load(CompressionCodecProvider.class).stream()
                .map(ServiceLoader.Provider::type)
                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(DeflateCodec.Provider.class, GzipCodecProvider.class,
                        SnappyCodec.Provider.class, ZstdCodec.Provider.class);
    }

    @Test
    public void codecsAreSortedByName() {
        final Codec lz4 = new NamedCodec("lz4");
        final Codec custom = new NamedCodec("x-lz4.v2+raw_1");
        final Map<String, Codec> codecs = codecsOf(List.of(
                new ZstdCodec.Provider(), new FixedProvider(custom), new FixedProvider(lz4), new GzipCodecProvider()));
        assertThat(codecs.keySet()).containsExactly("gzip", "lz4", "x-lz4.v2+raw_1", "zstd");
        assertThat(codecs.get("lz4")).isSameAs(lz4);
        assertThat(codecs.get("x-lz4.v2+raw_1")).isSameAs(custom);
        assertThat(codecs.get("zstd")).isSameAs(ZstdCodec.INSTANCE);
    }

    @Test
    public void providersWithMalformedNamesAreIgnored() {
        // upper case could never match, commas and whitespace separate names, and identity means no compression
        for (final String name : Arrays.asList("LZ4", "lz 4", "lz4,zstd", " lz4", "-lz4", "", "identity", null)) {
            assertThat(codecsOf(
                    List.of(new FixedProvider(new NamedCodec(name)), new GzipCodecProvider())).keySet())
                    .as("'%s'", name)
                    .containsExactly("gzip");
        }
        assertThat(codecsOf(List.of(new FixedProvider(null), new GzipCodecProvider())).keySet())
                .containsExactly("gzip");
    }

    @Test
    public void failingProvidersAreIgnored() {
        // a missing native library surfaces as an UnsatisfiedLinkError, and a failed static initializer as an
        // ExceptionInInitializerError
        for (final Throwable failure : List.of(new IllegalStateException("broken"),
                new UnsatisfiedLinkError("no zstd-jni in java.library.path"),
                new ExceptionInInitializerError("broken"))) {
            assertThat(codecsOf(
                    List.of(new FailingProvider(failure), new GzipCodecProvider())).keySet())
                    .as(failure.toString())
                    .containsExactly("gzip");
        }
    }

    @Test
    public void providersThatCannotBeLoadedAreIgnored(@TempDir final Path dir) throws IOException {
        // ServiceLoader throws from next() for a provider class that cannot be found, and from hasNext() for a
        // provider class whose superclass is missing; both come before a provider that loads
        final Path services = dir.resolve(CompressionCodecProvider.class.getName());
        Files.write(services, List.of("com.example.Missing", MissingBaseProvider.class.getName(),
                GzipCodecProvider.class.getName()));
        final ClassLoader loader = new MissingDependencyClassLoader(services.toUri().toURL());
        assertThat(codecsOf(ServiceLoader.load(CompressionCodecProvider.class, loader)).keySet())
                .containsExactly("gzip");
    }

    @Test
    public void deploymentProvidersReplaceBuiltInTypes() {
        final Codec gzip = new NamedCodec("gzip");
        // whatever the classpath order
        assertThat(codecsOf(List.of(new GzipCodecProvider(), new FixedProvider(gzip)))
                .get("gzip")).isSameAs(gzip);
        assertThat(codecsOf(List.of(new FixedProvider(gzip), new GzipCodecProvider()))
                .get("gzip")).isSameAs(gzip);
    }

    @Test
    public void firstClassNameWinsBetweenDeploymentProviders() {
        final Codec fixed = new NamedCodec("lz4");
        final Codec other = new NamedCodec("lz4");
        assertThat(FixedProvider.class.getName()).isLessThan(OtherProvider.class.getName());
        // whatever the classpath order
        assertThat(codecsOf(List.of(new FixedProvider(fixed), new OtherProvider(other)))
                .get("lz4")).isSameAs(fixed);
        assertThat(codecsOf(List.of(new OtherProvider(other), new FixedProvider(fixed)))
                .get("lz4")).isSameAs(fixed);
    }

    @Test
    public void registriesHoldOnlyTheGivenTypes() {
        final CompressorRegistry compressors = CompressionCodecs.compressorRegistry(List.of("zstd", "snappy"));
        assertThat(compressors.lookupCompressor("zstd")).isSameAs(ZstdCodec.INSTANCE);
        assertThat(compressors.lookupCompressor("snappy")).isNotNull();
        assertThat(compressors.lookupCompressor("gzip")).isNull();
        assertThat(compressors.lookupCompressor("identity")).isNotNull();

        final DecompressorRegistry decompressors =
                CompressionCodecs.decompressorRegistry(List.of("zstd", "snappy"), List.of("zstd", "gzip"));
        assertThat(decompressors.lookupDecompressor("zstd")).isSameAs(ZstdCodec.INSTANCE);
        assertThat(decompressors.lookupDecompressor("snappy")).isNotNull();
        assertThat(decompressors.lookupDecompressor("gzip")).isNull();
        // a type advertised but not registered is not advertised
        assertThat(decompressors.getAdvertisedMessageEncodings()).containsExactly("zstd");

        assertThatThrownBy(() -> CompressionCodecs.compressorRegistry(List.of("lz4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lz4");
    }

    @Test
    public void checkSupportedIgnoresCaseAndWhitespace() {
        for (final String name : CompressionCodecs.SUPPORTED) {
            assertThat(CompressionCodecs.checkSupported(name)).isEqualTo(name);
        }
        assertThat(CompressionCodecs.checkSupported(" ZStd ")).isEqualTo("zstd");
    }

    @Test
    public void checkSupportedRejectsUnknownNames() {
        assertThatThrownBy(() -> CompressionCodecs.checkSupported("lz4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported compression 'lz4'; supported values are deflate, gzip, snappy, zstd");
        // a single name cannot hold a list, nor name the absence of compression
        for (final String name : List.of("identity", "", " ", "zstd,gzip")) {
            assertThatThrownBy(() -> CompressionCodecs.checkSupported(name))
                    .as("'%s'", name)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void codecIsTheOneTheRegistriesHold() {
        final CompressorRegistry compressors = CompressionCodecs.compressorRegistry();
        final DecompressorRegistry decompressors = CompressionCodecs.decompressorRegistry(CompressionCodecs.SUPPORTED);
        for (final String name : CompressionCodecs.SUPPORTED) {
            final Codec codec = CompressionCodecs.codec(name);
            assertThat(codec.getMessageEncoding()).isEqualTo(name);
            assertThat(compressors.lookupCompressor(name)).as(name).isSameAs(codec);
            assertThat(decompressors.lookupDecompressor(name)).as(name).isSameAs(codec);
        }
        assertThat(CompressionCodecs.codec(" ZStd ")).isSameAs(CompressionCodecs.codec("zstd"));
        assertThatThrownBy(() -> CompressionCodecs.codec("lz4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lz4");
    }

    @Test
    public void everySupportedCodecRoundTrips() throws IOException {
        final CompressorRegistry compressors = CompressionCodecs.compressorRegistry();
        final DecompressorRegistry decompressors = CompressionCodecs.decompressorRegistry(CompressionCodecs.SUPPORTED);

        final byte[] random = new byte[256 * 1024];
        new Random(0).nextBytes(random);
        final byte[] repetitive = new byte[256 * 1024];
        for (int ii = 0; ii < repetitive.length; ++ii) {
            repetitive[ii] = (byte) (ii % 7);
        }

        for (final String name : CompressionCodecs.SUPPORTED) {
            final Compressor compressor = compressors.lookupCompressor(name);
            final Decompressor decompressor = decompressors.lookupDecompressor(name);
            assertThat(compressor).as(name).isNotNull();
            assertThat(decompressor).as(name).isNotNull();
            assertThat(compressor.getMessageEncoding()).isEqualTo(name);
            assertThat(decompressor.getMessageEncoding()).isEqualTo(name);

            for (final byte[] payload : List.of(new byte[0], random, repetitive)) {
                final byte[] compressed = compress(compressor, payload);
                assertThat(decompress(decompressor, compressed)).as(name).isEqualTo(payload);
            }
            assertThat(compress(compressor, repetitive).length).as(name).isLessThan(repetitive.length / 10);
        }
    }

    @Test
    public void decompressingStreamsReportAvailableDataUntilEof() throws IOException {
        // Arrow Flight's ArrowMessage only parses while available() > 0
        final CompressorRegistry compressors = CompressionCodecs.compressorRegistry();
        final DecompressorRegistry decompressors = CompressionCodecs.decompressorRegistry(CompressionCodecs.SUPPORTED);
        final byte[] payload = new byte[64 * 1024];
        new Random(1).nextBytes(payload);

        for (final String name : CompressionCodecs.SUPPORTED) {
            final byte[] compressed = compress(compressors.lookupCompressor(name), payload);
            try (final InputStream in =
                    decompressors.lookupDecompressor(name).decompress(new ByteArrayInputStream(compressed))) {
                assertThat(in.available()).as(name).isPositive();
                assertThat(in.readAllBytes()).as(name).isEqualTo(payload);
                assertThat(in.read()).as(name).isEqualTo(-1);
                assertThat(in.available()).as(name).isZero();
            }
        }
    }

    @Test
    public void decompressorRegistryAdvertisesOnlyRequestedCodecs() {
        final DecompressorRegistry zstdOnly = CompressionCodecs.decompressorRegistry(List.of("zstd"));
        assertThat(zstdOnly.getAdvertisedMessageEncodings()).containsExactly("zstd");
        // every supported codec can still be decoded
        for (final String name : CompressionCodecs.SUPPORTED) {
            assertThat(zstdOnly.lookupDecompressor(name)).as(name).isNotNull();
        }

        assertThat(CompressionCodecs.decompressorRegistry(List.of()).getAdvertisedMessageEncodings()).isEmpty();
        assertThat(CompressionCodecs.decompressorRegistry(CompressionCodecs.SUPPORTED).getAdvertisedMessageEncodings())
                .containsExactlyInAnyOrder("gzip", "zstd", "snappy", "deflate");
    }

    @Test
    public void decompressorRegistryRejectsUnknownCodecs() {
        assertThatThrownBy(() -> CompressionCodecs.decompressorRegistry(List.of("zstd", "lz4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lz4");
    }

    @Test
    public void parseListKeepsOrderAndNormalizes() {
        assertThat(CompressionCodecs.parseList(" ZSTD, snappy ,,gzip, zstd ")).containsExactly("zstd", "snappy",
                "gzip");
        assertThat(CompressionCodecs.parseList("zstd")).containsExactly("zstd");
        assertThat(CompressionCodecs.parseList(null)).isEmpty();
        assertThat(CompressionCodecs.parseList("  ")).isEmpty();
    }

    @Test
    public void parseListRejectsUnknownNames() {
        assertThatThrownBy(() -> CompressionCodecs.parseList("zstd,identity"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity")
                .hasMessageContaining("deflate, gzip, snappy, zstd");
    }

    @Test
    public void parseAcceptEncodingKeepsUnknownNames() {
        assertThat(CompressionCodecs.parseAcceptEncoding("identity, br, GZIP"))
                .containsExactly("identity", "br", "gzip");
        assertThat(CompressionCodecs.parseAcceptEncoding(null)).isEmpty();
    }

    /** A codec that does not compress, under any name. */
    private static final class NamedCodec implements Codec {
        private final String name;

        private NamedCodec(final String name) {
            this.name = name;
        }

        @Override
        public String getMessageEncoding() {
            return name;
        }

        @Override
        public OutputStream compress(final OutputStream os) {
            return os;
        }

        @Override
        public InputStream decompress(final InputStream is) {
            return is;
        }
    }

    private static class FixedProvider implements CompressionCodecProvider {
        private final Codec codec;

        private FixedProvider(final Codec codec) {
            this.codec = codec;
        }

        @Override
        public Codec codec() {
            return codec;
        }
    }

    /** a second provider class, whose class name sorts after {@link FixedProvider}'s */
    private static final class OtherProvider extends FixedProvider {
        private OtherProvider(final Codec codec) {
            super(codec);
        }
    }

    /** a provider whose {@link #codec()} fails, as when a codec's native library is missing */
    private static final class FailingProvider implements CompressionCodecProvider {
        private final Throwable failure;

        private FailingProvider(final Throwable failure) {
            this.failure = failure;
        }

        @Override
        public Codec codec() {
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (RuntimeException) failure;
        }
    }

    /** stands in for a class from a jar that a deployment's provider needs, but that is not on the classpath */
    private static class MissingBase {
    }

    /** a deployment's provider whose superclass {@link MissingDependencyClassLoader} cannot load */
    private static final class MissingBaseProvider extends MissingBase implements CompressionCodecProvider {
        @Override
        public Codec codec() {
            return Codec.Identity.NONE;
        }
    }

    /**
     * Lists only the providers in a given services file, and cannot load {@link MissingBase}, as when a deployment's
     * provider jar is on the classpath without a jar that the provider needs.
     */
    private static final class MissingDependencyClassLoader extends ClassLoader {
        private final URL services;

        private MissingDependencyClassLoader(final URL services) {
            super(CompressionCodecsTest.class.getClassLoader());
            this.services = services;
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            if (name.equals(MissingBase.class.getName())) {
                throw new ClassNotFoundException(name);
            }
            if (!name.equals(MissingBaseProvider.class.getName())) {
                return super.loadClass(name, resolve);
            }
            // defined here rather than by the parent, so that MissingBaseProvider's superclass is looked up through
            // this loader
            try (final InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                final byte[] bytes = in.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length);
            } catch (final IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }

        @Override
        public Enumeration<URL> getResources(final String name) throws IOException {
            return name.equals("META-INF/services/" + CompressionCodecProvider.class.getName())
                    ? Collections.enumeration(List.of(services))
                    : super.getResources(name);
        }
    }

    private static Map<String, Codec> codecsOf(final Iterable<CompressionCodecProvider> providers) {
        return CompressionCodecs.byEncodingName(providers);
    }


    private static byte[] compress(final Compressor compressor, final byte[] payload) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final OutputStream compressing = compressor.compress(out)) {
            compressing.write(payload);
        }
        return out.toByteArray();
    }

    private static byte[] decompress(final Decompressor decompressor, final byte[] payload) throws IOException {
        try (final InputStream in = decompressor.decompress(new ByteArrayInputStream(payload))) {
            return in.readAllBytes();
        }
    }
}
