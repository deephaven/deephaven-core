//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.materializers.BlobMaterializer;
import io.deephaven.parquet.base.materializers.PlainBinaryStringMaterializer;
import io.deephaven.parquet.base.materializers.PlainBinaryStringValuesReader;
import io.deephaven.parquet.base.materializers.StringMaterializer;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
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

    /** The case the decoder exists for: a PLAIN-encoded BINARY page, heap-backed, destined for Strings. */
    @Test
    void selectedForPlainBinaryStrings() {
        assertThat(usePlainBinaryStringReader(
                Encoding.PLAIN, PrimitiveTypeName.BINARY, StringMaterializer.FACTORY, HEAP)).isTrue();
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

    /**
     * The narrowing is a correctness requirement, not a tuning choice: PlainBinaryStringValuesReader implements only
     * bulk String decoding, so any other BINARY consumer handed one would throw from readBytes().
     */
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

    /**
     * The factory, not the materializer, picks the decode strategy. Falling through to {@link StringMaterializer} would
     * still produce correct values, just slower, so only a type assertion catches it.
     */
    @Test
    void factoryDispatchesOnReaderType() {
        final PlainBinaryStringValuesReader fast = new PlainBinaryStringValuesReader(HEAP);
        final BinaryPlainValuesReader stock = new BinaryPlainValuesReader();

        assertThat(StringMaterializer.FACTORY.makeMaterializerNonNull(fast, 1))
                .isInstanceOf(PlainBinaryStringMaterializer.class);
        assertThat(StringMaterializer.FACTORY.makeMaterializerWithNulls(fast, null, 1))
                .isInstanceOf(PlainBinaryStringMaterializer.class);

        assertThat(StringMaterializer.FACTORY.makeMaterializerNonNull(stock, 1))
                .isInstanceOf(StringMaterializer.class);
        assertThat(StringMaterializer.FACTORY.makeMaterializerWithNulls(stock, null, 1))
                .isInstanceOf(StringMaterializer.class);
    }
}
