//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.configuration.Configuration;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

/**
 * {@link PageMaterializer} implementation for {@link Instant Instants} stored as Int96s representing an Impala format
 * Timestamp (nanoseconds of day and Julian date encoded as 8 bytes and 4 bytes, respectively)
 */
public class InstantNanosFromInt96Materializer extends LongMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new InstantNanosFromInt96Materializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new InstantNanosFromInt96Materializer(dataReader, numValues);
        }
    };

    /*
     * Potential references/points of comparison for this algorithm: https://github.com/apache/iceberg/pull/1184/files
     * https://github.com/apache/arrow/blob/master/cpp/src/parquet/types.h (last retrieved as
     * https://github.com/apache/arrow/blob/d5a2aa2ffb1c2fc4f3ca48c829fcdba80ec67916/cpp/src/parquet/types.h)
     */
    private static final long NANOS_PER_DAY = 86400L * 1000 * 1000 * 1000;
    private static final int JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS = 2_440_588;
    private static long offset;
    static {
        final String referenceTimeZone =
                Configuration.getInstance().getStringWithDefault("deephaven.parquet.referenceTimeZone", "UTC");
        setReferenceTimeZone(referenceTimeZone);
    }

    private final ValuesReader dataReader;

    private InstantNanosFromInt96Materializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private InstantNanosFromInt96Materializer(ValuesReader dataReader, long nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    /**
     * Allows overriding the time zone to be used when interpreting Int96 timestamp values. Default is UTC. Can be set
     * globally with the parameter deephaven.parquet.referenceTimeZone. Valid values are time zone strings that would be
     * used in {@link DateTimeUtils#parseInstant(String) parseInstant}, such as NY.
     */
    private static void setReferenceTimeZone(@NotNull final String timeZone) {
        offset = DateTimeUtils.nanosOfDay(DateTimeUtils.parseInstant("1970-01-01T00:00:00 " + timeZone),
                ZoneId.of("UTC"), false);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = readInstantNanos();
        }
    }

    long readInstantNanos() {
        final ByteBuffer resultBuffer = ByteBuffer.wrap(dataReader.readBytes().getBytesUnsafe());
        resultBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        final long nanos = resultBuffer.getLong();
        final int julianDate = resultBuffer.getInt();
        return (julianDate - JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS) * (NANOS_PER_DAY) + nanos + offset;
    }
}
