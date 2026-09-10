//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MILLI;

public class LocalDateTimeFromMillisMaterializer extends ObjectMaterializerBase<LocalDateTime>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromMillisMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new LocalDateTimeFromMillisMaterializer(dataReader, numValues);
        }
    };

    /**
     * Converts milliseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param value milliseconds since Epoch
     * @return The input milliseconds from the Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    public static LocalDateTime convertValue(long value) {
        return LocalDateTime.ofEpochSecond(value / 1_000L, (int) ((value % 1_000L) * MILLI),
                ZoneOffset.UTC);
    }

    private final PageValueReader dataReader;

    private LocalDateTimeFromMillisMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateTimeFromMillisMaterializer(PageValueReader dataReader, LocalDateTime nullValue,
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
