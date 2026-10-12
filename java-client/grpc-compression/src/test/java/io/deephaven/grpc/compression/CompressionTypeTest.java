//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionTypeTest {

    @Test
    public void encodingNamesComeFromTheCodecs() {
        assertThat(CompressionType.ZSTD.encodingName()).isEqualTo("zstd");
        assertThat(CompressionType.GZIP.encodingName()).isEqualTo("gzip");
        assertThat(CompressionType.SNAPPY.encodingName()).isEqualTo("snappy");
        for (final CompressionType type : CompressionType.values()) {
            assertThat(type.encodingName()).isEqualTo(type.codec().getMessageEncoding());
        }
    }

    @Test
    public void fromEncodingNameIgnoresCaseAndWhitespace() {
        for (final CompressionType type : CompressionType.values()) {
            assertThat(CompressionType.fromEncodingName(type.encodingName())).isSameAs(type);
        }
        assertThat(CompressionType.fromEncodingName(" ZStd ")).isSameAs(CompressionType.ZSTD);
    }

    @Test
    public void fromEncodingNameRejectsUnknownNames() {
        assertThatThrownBy(() -> CompressionType.fromEncodingName("lz4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lz4")
                .hasMessageContaining("zstd, gzip, snappy");
        assertThatThrownBy(() -> CompressionType.fromEncodingName("identity"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
