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
                .setProperty(StringMaterializer.ALLOW_PLAIN_BINARY_STRING_DECODER_PROP, Boolean.toString(enabled));
    }

    /** Removes the property entirely, so {@link #onByDefault} sees the unset state rather than an explicit value. */
    @AfterEach
    void clearProperty() {
        Configuration.getInstance().setProperty(StringMaterializer.ALLOW_PLAIN_BINARY_STRING_DECODER_PROP, null);
    }

    /** With the property unset, the decoder is selected -- this is the shipped behaviour. */
    @Test
    void onByDefault() {
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isTrue();
    }

    /** The escape hatch back to parquet's BinaryPlainValuesReader. */
    @Test
    void disabledByProperty() {
        setEnabled(false);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
    }

    /**
     * Encoding is per data page, not per column, so a chunk that also has a dictionary page still reaches this
     * predicate with PLAIN pages. Dictionary-encoded pages themselves must never take the fast path.
     */
    @Test
    void notSelectedForDictionaryEncoding() {
        assertThat(usePlainBinaryStringReader(
                Encoding.RLE_DICTIONARY, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
        // noinspection deprecation
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN_DICTIONARY, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
    }

    /** Every other primitive type is excluded, so the decoder can only ever see BINARY pages. */
    @Test
    void notSelectedForNonBinaryTypes() {
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
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, BlobMaterializer.FACTORY, HEAP)).isFalse();
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, PageMaterializerFactory.NULL_FACTORY, HEAP)).isFalse();
    }

    /** A direct page buffer has no backing array, so the predicate must fall back to the stock reader. */
    @Test
    void notSelectedForDirectBuffers() {
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, DIRECT)).isFalse();
    }

    /** The property is read per page, so the escape hatch must take effect without a restart, and be reversible. */
    @Test
    void respondsToRuntimeChanges() {
        setEnabled(false);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isFalse();
        setEnabled(true);
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isTrue();
    }
}
