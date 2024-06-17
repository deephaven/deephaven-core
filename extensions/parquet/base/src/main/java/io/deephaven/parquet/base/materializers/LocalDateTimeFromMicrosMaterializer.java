//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.ParquetTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDateTime;

public class LocalDateTimeFromMicrosMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromMicrosPageMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateTimeFromMicrosPageMaterializer(dataReader, numValues);
        }
    };

    private static final class LocalDateTimeFromMicrosPageMaterializer extends LocalDateTimePageMaterializerBase
            implements PageMaterializer {

        final ValuesReader dataReader;

        private LocalDateTimeFromMicrosPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        private LocalDateTimeFromMicrosPageMaterializer(ValuesReader dataReader, LocalDateTime nullValue,
                int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = ParquetTimeUtils.epochMicrosToLocalDateTimeUTC(dataReader.readLong());
            }
        }
    }
}
