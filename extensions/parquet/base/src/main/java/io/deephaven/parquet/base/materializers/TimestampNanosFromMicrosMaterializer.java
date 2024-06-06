//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

public class TimestampNanosFromMicrosMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new TimestampNanosFromMicrosPageMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new TimestampNanosFromMicrosPageMaterializer(dataReader, numValues);
        }
    };

    private static final class TimestampNanosFromMicrosPageMaterializer extends LongPageMaterializerBase
            implements PageMaterializer {

        final ValuesReader dataReader;

        private TimestampNanosFromMicrosPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        private TimestampNanosFromMicrosPageMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return DateTimeUtils.microsToNanos(dataReader.readLong());
        }
    }
}
