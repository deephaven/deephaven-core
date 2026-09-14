//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalDateTimeFromMillisMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import java.time.LocalDateTime;

public class LocalDateTimeFromMicrosMaterializer extends ObjectMaterializerBase<LocalDateTime>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromMicrosMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new LocalDateTimeFromMicrosMaterializer(dataReader, numValues);
        }
    };

    private final PageValueReader dataReader;

    private LocalDateTimeFromMicrosMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateTimeFromMicrosMaterializer(PageValueReader dataReader, LocalDateTime nullValue,
            int numValues) {
        super(nullValue, new LocalDateTime[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = PageValueConversions.localDateTimeFromEpochMicros(dataReader.readLong());
        }
    }
}
