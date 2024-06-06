//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

public class LongFromUnsignedIntMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LongFromUnsignedIntPageMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LongFromUnsignedIntPageMaterializer(dataReader, numValues);
        }
    };

    private static final class LongFromUnsignedIntPageMaterializer extends LongPageMaterializerBase
            implements PageMaterializer {

        final ValuesReader dataReader;

        private LongFromUnsignedIntPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        private LongFromUnsignedIntPageMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return Integer.toUnsignedLong(dataReader.readInteger());
        }
    }
}
