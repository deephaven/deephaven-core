//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;

public interface PageMaterializerFactory {
    PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues);

    PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues);

    /**
     * Whether PLAIN-encoded BINARY pages destined for this factory should be read with
     * {@code PlainBinaryStringValuesReader} instead of parquet's {@code BinaryPlainValuesReader}.
     * <p>
     * Opting in per factory keeps the {@code readBytes()} call site monomorphic for every other BINARY consumer.
     */
    default boolean usePlainBinaryStringDecoder() {
        return false;
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
