//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalTimeFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalTime;

public class LocalTimeFromNanosMaterializer extends ObjectMaterializerBase<LocalTime> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromNanosMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromNanosMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private LocalTimeFromNanosMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalTimeFromNanosMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
        super(nullValue, new LocalTime[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = DateTimeUtils.nanosOfDayToLocalTime(dataReader.readLong());
        }
    }
}
