//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the factory builds. Which pages reach it is covered by {@code TestPlainBinaryStringReaderSelection}, which lives
 * with the page reader that makes that decision.
 */
class TestStringPageMaterializerFactory {

    private static final ByteBuffer HEAP = ByteBuffer.allocate(64);

    private final StringPageMaterializerFactory factory = new StringPageMaterializerFactory();

    @Test
    void suppliesThePlainBinaryReader() {
        assertThat(factory.makePlainBinaryValuesReader(HEAP)).isInstanceOf(PlainBinaryStringValuesReader.class);
    }

    /**
     * The factory, not the materializer, picks the decode strategy. Falling through to {@link StringMaterializer} would
     * still produce correct values, just slower, so only a type assertion catches it.
     */
    @Test
    void dispatchesOnReaderType() {
        final PlainBinaryStringValuesReader fast = new PlainBinaryStringValuesReader(HEAP);
        final BinaryPlainValuesReader stock = new BinaryPlainValuesReader();

        assertThat(factory.makeMaterializerNonNull(fast, 1)).isInstanceOf(PlainBinaryStringMaterializer.class);
        assertThat(factory.makeMaterializerWithNulls(fast, null, 1)).isInstanceOf(PlainBinaryStringMaterializer.class);

        assertThat(factory.makeMaterializerNonNull(stock, 1)).isInstanceOf(StringMaterializer.class);
        assertThat(factory.makeMaterializerWithNulls(stock, null, 1)).isInstanceOf(StringMaterializer.class);
    }
}
