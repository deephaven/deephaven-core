//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.nio.ByteBuffer;

/**
 * Builds String materializers, reading PLAIN-encoded BINARY pages with its own {@link ValuesReader} rather than
 * parquet's. The page reader offers a page to this type only when the page is one that reader can consume.
 */
public class StringPageMaterializerFactory implements PageMaterializerFactory {

    @Override
    public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
        return dataReader instanceof PlainBinaryStringValuesReader
                ? new PlainBinaryStringMaterializer(
                        (PlainBinaryStringValuesReader) dataReader, (String) nullValue, numValues)
                : new StringMaterializer(dataReader, (String) nullValue, numValues);
    }

    @Override
    public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
        return dataReader instanceof PlainBinaryStringValuesReader
                ? new PlainBinaryStringMaterializer((PlainBinaryStringValuesReader) dataReader, numValues)
                : new StringMaterializer(dataReader, numValues);
    }

    /**
     * @param in a heap-backed page buffer, positioned past the repetition and definition levels
     */
    public ValuesReader makePlainBinaryValuesReader(final ByteBuffer in) {
        return new PlainBinaryStringValuesReader(in);
    }
}
