//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalTimeFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import java.time.LocalTime;

public class LocalTimeFromMillisMaterializer extends ObjectMaterializerBase<LocalTime> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new LocalTimeFromMillisMaterializer(dataReader, numValues);
        }
    };

    private final PageValueReader dataReader;

    private LocalTimeFromMillisMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalTimeFromMillisMaterializer(PageValueReader dataReader, LocalTime nullValue, int numValues) {
        super(nullValue, new LocalTime[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = PageValueConversions.localTimeFromMillisOfDay(dataReader.readInteger());
        }
    }
}
