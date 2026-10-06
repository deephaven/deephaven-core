//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

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
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionCodecsTest {

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
                .containsExactlyInAnyOrder("gzip", "zstd", "snappy");
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
                .hasMessageContaining("gzip, zstd, snappy");
    }

    @Test
    public void parseAcceptEncodingKeepsUnknownNames() {
        assertThat(CompressionCodecs.parseAcceptEncoding("identity, deflate, GZIP"))
                .containsExactly("identity", "deflate", "gzip");
        assertThat(CompressionCodecs.parseAcceptEncoding(null)).isEmpty();
    }

    @Test
    public void chooseUsesFirstAllowedCodecTheClientAccepts() {
        final List<String> zstdOnly = List.of("zstd");
        final List<String> all = List.of("zstd", "snappy", "gzip");

        assertThat(CompressionCodecs.choose(zstdOnly, Set.of("gzip"))).isEmpty();
        assertThat(CompressionCodecs.choose(zstdOnly, Set.of("gzip", "zstd"))).contains("zstd");
        assertThat(CompressionCodecs.choose(all, Set.of("gzip", "snappy"))).contains("snappy");
        assertThat(CompressionCodecs.choose(all, Set.of("identity", "deflate", "gzip"))).contains("gzip");
        assertThat(CompressionCodecs.choose(all, Set.of())).isEmpty();
        assertThat(CompressionCodecs.choose(List.of(), Set.of("gzip", "zstd"))).isEqualTo(Optional.empty());
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
