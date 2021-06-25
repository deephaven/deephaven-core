package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Parquet {@link ToPage} implementation for {@link DBDateTime}s stored as Int96s representing an Impala
 * format Timestamp (nanoseconds of day and Julian date encoded as 8 bytes and 4 bytes, respectively)
 *
 */
public class ToDBDateTimePageFromInt96<ATTR extends Attributes.Any> implements ToPage<ATTR, long[]> {
    /*
     * Potential references/points of comparison for this algorithm:
     *   https://github.com/apache/iceberg/pull/1184/files
     *   https://github.com/apache/arrow/blob/master/cpp/src/parquet/types.h
     *   (last retrieved as https://github.com/apache/arrow/blob/d5a2aa2ffb1c2fc4f3ca48c829fcdba80ec67916/cpp/src/parquet/types.h)
     */
    @SuppressWarnings("rawtypes")
    private static final ToDBDateTimePageFromInt96 INSTANCE = new ToDBDateTimePageFromInt96<>();
    private static final long NANOS_PER_DAY = 86400L * 1000 * 1000 * 1000;
    private static final int JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS = 2_440_588;
    private static long offset;
    static {
        final String referenceTimeZone = Configuration.getInstance().getStringWithDefault("deephaven.parquet.referenceTimeZone","UTC");
        setReferenceTimeZone(referenceTimeZone);
    }

    public static <ATTR extends Attributes.Any> ToDBDateTimePageFromInt96<ATTR> create(@NotNull Class<?> nativeType) {
        if (DBDateTime.class.equals(nativeType)) {
            //noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a DBDateTime column is " + nativeType.getCanonicalName());
    }

    private ToDBDateTimePageFromInt96() {
    }

    /**
     * Allows overriding the time zone to be used when interpreting Int96 timestamp values.
     * Default is UTC. Can be set globally with the parameter deephaven.parquet.referenceTimeZone.
     * Valid values are time zone Strings which would be used in convertDateTime, such as NY.
     * @param timeZone
     */
    public static void setReferenceTimeZone(@NotNull final String timeZone) {
        offset = DBTimeUtils.nanosOfDay(DBTimeUtils.convertDateTime("1970-01-01T00:00:00 " + timeZone), DBTimeZone.TZ_UTC);
    }

    @Override
    @NotNull
    public final Class<Long> getNativeType() {
        return long.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Long;
    }

    @Override
    public Object nullValue() { return null;  }

    @Override
    @NotNull
    public final Class<DBDateTime> getNativeComponentType() {
        return DBDateTime.class;
    }

    @Override
    public final long[] convertResult(@NotNull final Object result) {
        // result is delivered as an array of Binary[12]
        final Binary[] results = (Binary[])result;
        final int resultLength = results.length;
        final long[] resultLongs = new long[resultLength];

        for (int ri = 0; ri < resultLength; ++ri) {
            final ByteBuffer resultBuffer = ByteBuffer.wrap(results[ri].getBytesUnsafe());
            resultBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            final long nanos = resultBuffer.getLong();
            final int julianDate = resultBuffer.getInt();
            resultLongs[ri] = (julianDate - JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS) * (NANOS_PER_DAY) + nanos + offset;
        }
        return resultLongs;
    }

    @Override
    @NotNull
    public final DbArray<DBDateTime> makeDbArray(@NotNull final long[] result) {
        final DBDateTime[] to = new DBDateTime[result.length];

        final int resultLength = result.length;
        for (int ri = 0; ri < resultLength; ++ri) {
            to[ri] = DBTimeUtils.nanosToTime(result[ri]);
        }

        return new DbArrayDirect<>(to);
    }
}
