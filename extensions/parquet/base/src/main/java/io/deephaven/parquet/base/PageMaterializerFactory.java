//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

public interface PageMaterializerFactory {
    PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues);

    PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues);

    PageMaterializerFactory NULL_FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue,
                int numValues) {
            throw new UnsupportedOperationException("Does not support materializing pages");
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            throw new UnsupportedOperationException("Does not support materializing pages");
        }
    };
}
