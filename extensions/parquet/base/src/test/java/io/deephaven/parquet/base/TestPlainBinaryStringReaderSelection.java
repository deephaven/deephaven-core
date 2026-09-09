//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.configuration.Configuration;
import io.deephaven.parquet.base.materializers.BlobMaterializer;
import io.deephaven.parquet.base.materializers.StringMaterializer;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.deephaven.parquet.base.ColumnPageReaderImpl.usePlainBinaryStringReader;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The selection rule for {@code PlainBinaryStringValuesReader}. Verified here rather than through a parquet read
 * because class loading happens whether or not the branch is taken, so it is not evidence the path fired.
 */
class TestPlainBinaryStringReaderSelection {

    private static final ByteBuffer HEAP = ByteBuffer.allocate(64);
    private static final ByteBuffer DIRECT = ByteBuffer.allocateDirect(64);

    private void setEnabled(final boolean enabled) {
        Configuration.getInstance()
                .setProperty(StringMaterializer.USE_PLAIN_BINARY_STRING_DECODER_PROP, Boolean.toString(enabled));
    }

    @AfterEach
    void clearProperty() {
        setEnabled(false);
    }

    @Test
    void selectedForPlainBinaryStringsWhenEnabled() {
        setEnabled(true);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isTrue();
    }

    @Test
    void offByDefault() {
        setEnabled(false);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
    }

    @Test
    void notSelectedForDictionaryEncoding() {
        setEnabled(true);
        assertThat(usePlainBinaryStringReader(
                Encoding.RLE_DICTIONARY, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN_DICTIONARY, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
    }

    @Test
    void notSelectedForNonBinaryTypes() {
        setEnabled(true);
        for (final PrimitiveTypeName type : PrimitiveTypeName.values()) {
            if (type == PrimitiveTypeName.BINARY) {
                continue;
            }
            assertThat(usePlainBinaryStringReader(Encoding.PLAIN, type, StringMaterializer.FACTORY, HEAP))
                    .as("type %s", type)
                    .isFalse();
        }
    }

    /** The narrowing that keeps other BINARY consumers' readBytes() call site monomorphic. */
    @Test
    void notSelectedForOtherBinaryMaterializers() {
        setEnabled(true);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, BlobMaterializer.FACTORY, HEAP)).isFalse();
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, PageMaterializerFactory.NULL_FACTORY, HEAP)).isFalse();
    }

    @Test
    void notSelectedForDirectBuffers() {
        setEnabled(true);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, DIRECT)).isFalse();
    }

    /** The property is read per page, so flipping it at runtime must take effect without a restart. */
    @Test
    void respondsToRuntimeChanges() {
        setEnabled(true);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isTrue();
        setEnabled(false);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
    }
}
