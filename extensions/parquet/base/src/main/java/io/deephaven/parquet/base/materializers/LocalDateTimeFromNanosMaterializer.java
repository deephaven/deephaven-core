//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalDateTimeFromMillisMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.NANO;

public class LocalDateTimeFromNanosMaterializer extends ObjectMaterializerBase<LocalDateTime>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateTimeFromNanosMaterializer(dataReader, (LocalDateTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateTimeFromNanosMaterializer(dataReader, numValues);
        }
    };

    /**
     * Converts nanoseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param value nanoseconds since Epoch
     * @return The input nanoseconds from the Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    public static LocalDateTime convertValue(long value) {
        return LocalDateTime.ofEpochSecond(value / 1_000_000_000L, (int) ((value % 1_000_000_000L) * NANO),
                ZoneOffset.UTC);
    }

    private final ValuesReader dataReader;

    private LocalDateTimeFromNanosMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateTimeFromNanosMaterializer(ValuesReader dataReader, LocalDateTime nullValue,
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
