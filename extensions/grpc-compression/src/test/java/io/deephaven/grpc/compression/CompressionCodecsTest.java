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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
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
        final Map<String, Codec> codecs = CompressionCodecs.byEncodingName(List.of(
                new ZstdCodec.Provider(), new FixedProvider(custom), new FixedProvider(lz4), new GzipCodecProvider()));
        assertThat(codecs.keySet()).containsExactly("gzip", "lz4", "x-lz4.v2+raw_1", "zstd");
        assertThat(codecs.get("lz4")).isSameAs(lz4);
        assertThat(codecs.get("x-lz4.v2+raw_1")).isSameAs(custom);
        assertThat(codecs.get("zstd")).isSameAs(ZstdCodec.INSTANCE);
    }

    @Test
    public void providedNamesMustBeWellFormed() {
        // upper case could never match, commas and whitespace separate names, and identity means no compression
        for (final String name : Arrays.asList("LZ4", "lz 4", "lz4,zstd", " lz4", "-lz4", "", "identity", null)) {
            assertThatThrownBy(() -> CompressionCodecs.byEncodingName(List.of(new FixedProvider(new NamedCodec(name)))))
                    .as("'%s'", name)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(FixedProvider.class.getName());
        }
        assertThatThrownBy(() -> CompressionCodecs.byEncodingName(List.of(new FixedProvider(null))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(FixedProvider.class.getName() + " provides no codec");
    }

    @Test
    public void providedNamesMustBeNew() {
        assertThatThrownBy(() -> CompressionCodecs.byEncodingName(
                List.of(new GzipCodecProvider(), new FixedProvider(new NamedCodec("gzip")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Compression type 'gzip' is provided by both " + GzipCodecProvider.class.getName() + " and "
                        + FixedProvider.class.getName());
        assertThatThrownBy(() -> CompressionCodecs.byEncodingName(
                List.of(new FixedProvider(new NamedCodec("lz4")), new OtherProvider(new NamedCodec("lz4")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Compression type 'lz4' is provided by both " + FixedProvider.class.getName() + " and "
                        + OtherProvider.class.getName());
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

    /** a second provider class, so that errors about a name two providers share can name both */
    private static final class OtherProvider extends FixedProvider {
        private OtherProvider(final Codec codec) {
            super(codec);
        }
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
