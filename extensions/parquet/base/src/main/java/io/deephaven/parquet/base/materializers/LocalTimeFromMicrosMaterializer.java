//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalTime;

public class LocalTimeFromMicrosMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMicrosPageMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromMicrosPageMaterializer(dataReader, numValues);
        }
    };

    private static final class LocalTimeFromMicrosPageMaterializer extends LocalTimePageMaterializerBase
            implements PageMaterializer {

        final ValuesReader dataReader;

        private LocalTimeFromMicrosPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        private LocalTimeFromMicrosPageMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        LocalTime readNext() {
            return DateTimeUtils.microsOfDayToLocalTime(dataReader.readLong());
        }
    }
}
