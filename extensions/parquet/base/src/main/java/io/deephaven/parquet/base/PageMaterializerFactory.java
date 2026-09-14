//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public interface PageMaterializerFactory {
    PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues);

    PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues);

    /**
     * A reader for a PLAIN-encoded BINARY page, or {@code null} to use parquet's. Only return one the materializer this
     * factory builds can consume.
     *
     * @param in the page buffer, positioned past the repetition and definition levels
     */
    @Nullable
    default ValuesReader maybeMakePlainBinaryValuesReader(final ByteBuffer in) {
        return null;
    }

    PageMaterializerFactory NULL_FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            throw new UnsupportedOperationException("Does not support materializing pages");
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            throw new UnsupportedOperationException("Does not support materializing pages");
        }
    };
}
