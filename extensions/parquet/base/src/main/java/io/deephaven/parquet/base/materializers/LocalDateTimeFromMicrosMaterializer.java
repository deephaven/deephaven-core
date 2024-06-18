//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalDateTimeFromMillisMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.ParquetTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDateTime;

public class LocalDateTimeFromMicrosMaterializer extends LocalDateTimeMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromMicrosMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateTimeFromMicrosMaterializer(dataReader, numValues);
        }
    };

    final ValuesReader dataReader;

    private LocalDateTimeFromMicrosMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateTimeFromMicrosMaterializer(ValuesReader dataReader, LocalDateTime nullValue,
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
