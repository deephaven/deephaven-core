//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TimestampNanosFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

public class TimestampNanosFromMillisMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new TimestampNanosFromMillisPageMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new TimestampNanosFromMillisPageMaterializer(dataReader, numValues);
        }
    };

    private static final class TimestampNanosFromMillisPageMaterializer extends LongPageMaterializerBase
            implements PageMaterializer {

        final ValuesReader dataReader;

        private TimestampNanosFromMillisPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        private TimestampNanosFromMillisPageMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        long readLong() {
            return DateTimeUtils.millisToNanos(dataReader.readLong());
        }
    }
}
