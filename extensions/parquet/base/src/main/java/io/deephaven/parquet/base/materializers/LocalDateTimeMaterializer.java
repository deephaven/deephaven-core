//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.ParquetTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.function.LongFunction;

public class LocalDateTimeMaterializer {

    public static final PageMaterializerFactory FromMillisFactory =
            create(ParquetTimeUtils::epochMillisToLocalDateTimeUTC);
    public static final PageMaterializerFactory FromMicrosFactory =
            create(ParquetTimeUtils::epochMicrosToLocalDateTimeUTC);
    public static final PageMaterializerFactory FromNanosFactory =
            create(ParquetTimeUtils::epochNanosToLocalDateTimeUTC);

    private static PageMaterializerFactory create(final LongFunction<LocalDateTime> converter) {
        return new PageMaterializerFactory() {
            @Override
            public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue,
                    int numValues) {
                return new LocalDateTimePageMaterializer(dataReader, (LocalDateTime) nullValue, numValues, converter);
            }

            @Override
            public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
                return new LocalDateTimePageMaterializer(dataReader, numValues, converter);
            }
        };
    }

    private static final class LocalDateTimePageMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final LocalDateTime nullValue;
        final LocalDateTime[] data;
        final LongFunction<LocalDateTime> converter;

        private LocalDateTimePageMaterializer(ValuesReader dataReader, int numValues,
                LongFunction<LocalDateTime> converter) {
            this(dataReader, null, numValues, converter);
        }

        private LocalDateTimePageMaterializer(ValuesReader dataReader, LocalDateTime nullValue, int numValues,
                LongFunction<LocalDateTime> converter) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new LocalDateTime[numValues];
            this.converter = converter;
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = converter.apply(dataReader.readLong());
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }
}
