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
import java.time.ZoneOffset;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MICRO;

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

    /**
     * Converts microseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param value microseconds since Epoch
     * @return The input microseconds from the Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    public static LocalDateTime convertValue(long value) {
        return LocalDateTime.ofEpochSecond(value / 1_000_000L, (int) ((value % 1_000_000L) * MICRO),
                ZoneOffset.UTC);
    }

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
            data[ii] = convertValue(dataReader.readLong());
        }
    }
}
