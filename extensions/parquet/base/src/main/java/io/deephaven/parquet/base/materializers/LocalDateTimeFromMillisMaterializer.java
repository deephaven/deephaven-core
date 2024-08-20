//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.ParquetTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDateTime;

public class LocalDateTimeFromMillisMaterializer extends ObjectMaterializerBase<LocalDateTime>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromMillisMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateTimeFromMillisMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private LocalDateTimeFromMillisMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateTimeFromMillisMaterializer(ValuesReader dataReader, LocalDateTime nullValue,
            int numValues) {
        super(nullValue, new LocalDateTime[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = ParquetTimeUtils.epochMillisToLocalDateTimeUTC(dataReader.readLong());
        }
    }
}
