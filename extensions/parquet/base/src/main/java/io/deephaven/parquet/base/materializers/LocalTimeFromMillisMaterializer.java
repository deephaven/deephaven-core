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

public class LocalTimeFromMillisMaterializer extends LocalTimeMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, numValues);
        }
    };

    final ValuesReader dataReader;

    private LocalTimeFromMillisMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalTimeFromMillisMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = DateTimeUtils.millisOfDayToLocalTime(dataReader.readInteger());
        }
    }
}
