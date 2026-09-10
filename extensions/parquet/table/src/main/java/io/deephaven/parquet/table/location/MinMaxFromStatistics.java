//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.parquet.base.materializers.PageValueConversions;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;


/**
 * This is a utility class that provides methods to extract minimum and maximum values from Parquet statistics based on
 * the column's logical and primitive types.
 * <p>
 * This class assumes that the statistics provided are valid and {@link ParquetPushdownUtils#areStatisticsUsable
 * usable}.
 * <p>
 * The extracted min/max values describe the <i>non-null</i> values in a row group; parquet statistics never fold nulls
 * into them. Nothing here, and nothing in the handlers built on top of it, can therefore see a row group's null rows.
 * Callers that need to account for nulls must consult the null count separately, via
 * {@link ParquetPushdownUtils#isProvenFreeOfNulls}.
 * <p>
 * The general structure is that based on the type requested by user, we first try to extract the min/max values from
 * the logical type, and if that fails, we try to extract them from the primitive type. If both fail, we return
 * {@code false}.
 */
final class MinMaxFromStatistics {

    private static void verifyPrimitive(
            final Statistics<?> stats,
            final PrimitiveType.PrimitiveTypeName expected) {
        final PrimitiveType.PrimitiveTypeName actual = stats.type().getPrimitiveTypeName();
        if (actual != expected) {
            throw new IllegalStateException(String.format(
                    "Unexpected primitive type %s (expected %s) for logical type %s",
                    actual, expected, stats.type().getLogicalTypeAnnotation()));
        }
    }

    /**
     * Attempts to retrieve the minimum and maximum bytes from the given {@code statistics}.
     * <p>
     * Byte values can be read from parquet statistics of logical type INT_8.
     */
    static boolean getMinMaxForBytes(
            @NotNull final Statistics<?> statistics,
            @NotNull final ByteConsumer minSetter,
            @NotNull final ByteConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
            if (intLogicalType.isSigned() && intLogicalType.getBitWidth() == 8) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                final IntStatistics intStats = (IntStatistics) statistics;
                minSetter.accept(PageValueConversions.byteFromInt(intStats.getMin()));
                maxSetter.accept(PageValueConversions.byteFromInt(intStats.getMax()));
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum characters from the given {@code statistics}.
     * <p>
     * Character values can be read from parquet statistics of logical type UINT_8, UINT_16.
     */
    static boolean getMinMaxForChars(
            @NotNull final Statistics<?> statistics,
            @NotNull final CharConsumer minSetter,
            @NotNull final CharConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
            if (!intLogicalType.isSigned()) {
                final int bitWidth = intLogicalType.getBitWidth();
                if (bitWidth == 8 || bitWidth == 16) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.charFromInt(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.charFromInt(intStats.getMax()));
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum shorts from the given {@code statistics}.
     * <p>
     * Short values can be read from parquet statistics of logical type INT_8, INT_16, UINT_8, or primitive type
     * BOOLEAN.
     */
    static boolean getMinMaxForShorts(
            @NotNull final Statistics<?> statistics,
            @NotNull final ShortConsumer minSetter,
            @NotNull final ShortConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetColType.getPrimitiveTypeName();
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
            final boolean isSigned = intLogicalType.isSigned();
            final int bitWidth = intLogicalType.getBitWidth();
            if (isSigned && (bitWidth == 8 || bitWidth == 16)) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                final IntStatistics intStats = (IntStatistics) statistics;
                minSetter.accept(PageValueConversions.shortFromInt(intStats.getMin()));
                maxSetter.accept(PageValueConversions.shortFromInt(intStats.getMax()));
                return true;
            } else if (!isSigned && bitWidth == 8) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                final IntStatistics intStats = (IntStatistics) statistics;
                minSetter.accept(PageValueConversions.shortFromUnsignedByte(intStats.getMin()));
                maxSetter.accept(PageValueConversions.shortFromUnsignedByte(intStats.getMax()));
                return true;
            }
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            final BooleanStatistics booleanStatistics = (BooleanStatistics) statistics;
            minSetter.accept(PageValueConversions.shortFromBoolean(booleanStatistics.getMin()));
            maxSetter.accept(PageValueConversions.shortFromBoolean(booleanStatistics.getMax()));
            return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum integers from the given {@code statistics}.
     * <p>
     * Integer values can be read from parquet statistics of logical type INT_8, INT_16, INT_32, UINT_8, UINT16, or
     * primitive type BOOLEAN, INT_32.
     */
    static boolean getMinMaxForInts(
            @NotNull final Statistics<?> statistics,
            @NotNull final IntConsumer minSetter,
            @NotNull final IntConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetColType.getPrimitiveTypeName();
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
            final boolean isSigned = intLogicalType.isSigned();
            final int bitWidth = intLogicalType.getBitWidth();
            if (isSigned && (bitWidth == 8 || bitWidth == 16 || bitWidth == 32)) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                final IntStatistics intStats = (IntStatistics) statistics;
                minSetter.accept(intStats.getMin());
                maxSetter.accept(intStats.getMax());
                return true;
            } else if (!isSigned) {
                if (bitWidth == 8) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.intFromUnsignedByte(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.intFromUnsignedByte(intStats.getMax()));
                    return true;
                } else if (bitWidth == 16) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.intFromUnsignedShort(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.intFromUnsignedShort(intStats.getMax()));
                    return true;
                }
            }
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            final BooleanStatistics booleanStats = (BooleanStatistics) statistics;
            minSetter.accept(PageValueConversions.intFromBoolean(booleanStats.getMin()));
            maxSetter.accept(PageValueConversions.intFromBoolean(booleanStats.getMax()));
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
            final IntStatistics intStats = (IntStatistics) statistics;
            minSetter.accept(intStats.getMin());
            maxSetter.accept(intStats.getMax());
            return true;
        }
        return false;
    }


    /**
     * Attempts to retrieve the minimum and maximum longs from the given {@code statistics}.
     * <p>
     * Long values can be read from parquet statistics of logical type INT_8, INT_16, INT_32, INT_64, UINT_8, UINT_16,
     * UINT_32, or primitive type BOOLEAN, INT_32, INT_64.
     */
    static boolean getMinMaxForLongs(
            @NotNull final Statistics<?> statistics,
            @NotNull final LongConsumer minSetter,
            @NotNull final LongConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetColType.getPrimitiveTypeName();
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
            final boolean isSigned = intLogicalType.isSigned();
            final int bitWidth = intLogicalType.getBitWidth();
            if (isSigned) {
                if (bitWidth == 8 || bitWidth == 16 || bitWidth == 32) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(intStats.getMin());
                    maxSetter.accept(intStats.getMax());
                    return true;
                } else if (bitWidth == 64) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                    final LongStatistics longStats = (LongStatistics) statistics;
                    minSetter.accept(longStats.getMin());
                    maxSetter.accept(longStats.getMax());
                    return true;
                }
            } else {
                if (bitWidth == 8) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.longFromUnsignedByte(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.longFromUnsignedByte(intStats.getMax()));
                    return true;
                } else if (bitWidth == 16) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.longFromUnsignedShort(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.longFromUnsignedShort(intStats.getMax()));
                    return true;
                } else if (bitWidth == 32) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.longFromUnsignedInt(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.longFromUnsignedInt(intStats.getMax()));
                    return true;
                }
            }
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            final BooleanStatistics booleanStats = (BooleanStatistics) statistics;
            minSetter.accept(PageValueConversions.longFromBoolean(booleanStats.getMin()));
            maxSetter.accept(PageValueConversions.longFromBoolean(booleanStats.getMax()));
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
            final IntStatistics intStats = (IntStatistics) statistics;
            minSetter.accept(intStats.getMin());
            maxSetter.accept(intStats.getMax());
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
            final LongStatistics longStats = (LongStatistics) statistics;
            minSetter.accept(longStats.getMin());
            maxSetter.accept(longStats.getMax());
            return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum floats from the given {@code statistics}.
     * <p>
     * Float values can be read from parquet statistics of primitive type FLOAT.
     */
    static boolean getMinMaxForFloats(
            @NotNull final Statistics<?> statistics,
            @NotNull final FloatConsumer minSetter,
            @NotNull final FloatConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetColType.getPrimitiveTypeName();
        if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT) {
            final float minFloat = ((FloatStatistics) statistics).getMin();
            final float maxFloat = ((FloatStatistics) statistics).getMax();
            if (Float.isNaN(minFloat) || Float.isNaN(maxFloat)) {
                // NaN is not a valid min/max value and should have been handled automatically by the Builder logic,
                // so we return empty
                return false;
            }
            minSetter.accept(minFloat);
            maxSetter.accept(maxFloat);
            return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum doubles from the given {@code statistics}.
     * <p>
     * Double values can be read from parquet statistics of primitive type FLOAT, DOUBLE.
     */
    static boolean getMinMaxForDoubles(
            @NotNull final Statistics<?> statistics,
            @NotNull final DoubleConsumer minSetter,
            @NotNull final DoubleConsumer maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetColType.getPrimitiveTypeName();
        if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT) {
            final float minFloat = ((FloatStatistics) statistics).getMin();
            final float maxFloat = ((FloatStatistics) statistics).getMax();
            if (Float.isNaN(minFloat) || Float.isNaN(maxFloat)) {
                // NaN is not a valid min/max value and should have been handled automatically by the Builder logic,
                // so we return empty
                return false;
            }
            minSetter.accept(minFloat);
            maxSetter.accept(maxFloat);
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE) {
            final double minDouble = ((DoubleStatistics) statistics).getMin();
            final double maxDouble = ((DoubleStatistics) statistics).getMax();
            if (Double.isNaN(minDouble) || Double.isNaN(maxDouble)) {
                // NaN is not a valid min/max value and should have been handled automatically by the Builder logic,
                // so we return empty
                return false;
            }
            minSetter.accept(minDouble);
            maxSetter.accept(maxDouble);
            return true;
        }
        return false;
    }

    /**
     * Whether {@link #getMinMaxForComparable} can decode statistics for {@code columnType}. Knowable from the type
     * alone, so a handler can decline at evaluator-creation time rather than failing once per row group.
     * <p>
     * {@link String} is absent by design rather than for want of a decoder: {@link #getMinMaxForStrings} reads those
     * statistics perfectly well, but only as bytes. Parquet orders them by unsigned bytes while the Comparable path
     * would compare the decoded values with {@link String#compareTo} (UTF-16), a different order, so String columns are
     * routed to {@link StringPushdownHandler} ahead of the Comparable handlers instead; see
     * {@link StatisticsEvaluator#HANDLERS}.
     */
    static boolean canDecodeComparable(final Class<?> columnType) {
        return columnType == Instant.class
                || columnType == LocalDateTime.class
                || columnType == LocalDate.class
                || columnType == LocalTime.class;
    }

    static boolean getMinMaxForComparable(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<Comparable<?>> minSetter,
            @NotNull final Consumer<Comparable<?>> maxSetter,
            final Class<?> columnType) {
        if (columnType == Instant.class) {
            return getMinMaxForInstants(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == LocalDateTime.class) {
            return getMinMaxForLocalDateTimes(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == LocalDate.class) {
            return getMinMaxForLocalDates(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == LocalTime.class) {
            return getMinMaxForLocalTimes(statistics, minSetter::accept, maxSetter::accept);
        }
        // TODO (DH-19666): Add support for more types. Boolean is the cheapest: the format defines its column order
        // as "false, true", Boolean is Comparable, and Deephaven writes a native Parquet BOOLEAN, so `flag == true`
        // could prune where today it does not. BigDecimal and BigInteger are the other candidates.
        //
        // Note that adding a type here requires adding it to canDecodeComparable above as well, or the new support
        // will never be reached -- the handlers decline undecodable types at evaluator-creation time. String is the
        // one type deliberately left out of both; see canDecodeComparable.
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum for a STRING/ENUM column, as raw bytes.
     * <p>
     * Parquet defines the extremes for these columns under unsigned byte-wise order, which is not Java's
     * {@link String#compareTo} order, so the bytes must be compared as bytes. The values are also permitted to be
     * <i>truncated</i> bounds rather than values present in the data: a truncated minimum is byte-wise less than or
     * equal to the true minimum and a truncated maximum byte-wise greater than or equal to the true maximum, which
     * keeps them valid bounds but means neither is necessarily a value in the row group, nor even valid UTF-8.
     */
    static boolean getMinMaxForStrings(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<byte[]> minSetter,
            @NotNull final Consumer<byte[]> maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.BINARY);
            minSetter.accept(statistics.getMinBytes());
            maxSetter.accept(statistics.getMaxBytes());
            return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum values for instants from the given {@code statistics}.
     * <p>
     * Instant values can be read from parquet statistics of logical type TIMESTAMP (when adjusted to UTC)
     */
    static boolean getMinMaxForInstants(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<Instant> minSetter,
            @NotNull final Consumer<Instant> maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
            if (timestampLogicalType.isAdjustedToUTC()) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                final long minLong = ((LongStatistics) statistics).getMin();
                final long maxLong = ((LongStatistics) statistics).getMax();
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        minSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                PageValueConversions.instantNanosFromEpochMillis(minLong)));
                        maxSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                PageValueConversions.instantNanosFromEpochMillis(maxLong)));
                        return true;
                    case MICROS:
                        minSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                PageValueConversions.instantNanosFromEpochMicros(minLong)));
                        maxSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                PageValueConversions.instantNanosFromEpochMicros(maxLong)));
                        return true;
                    case NANOS:
                        minSetter.accept(ParquetPushdownUtils.epochNanosToInstant(minLong));
                        maxSetter.accept(ParquetPushdownUtils.epochNanosToInstant(maxLong));
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum values for {@link LocalDateTime} from given {@code statistics}.
     * <p>
     * LocalDateTime values can be read from parquet statistics of logical type TIMESTAMP (when not adjusted to UTC).
     */
    static boolean getMinMaxForLocalDateTimes(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<LocalDateTime> minSetter,
            @NotNull final Consumer<LocalDateTime> maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
            if (!timestampLogicalType.isAdjustedToUTC()) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                final long minLong = ((LongStatistics) statistics).getMin();
                final long maxLong = ((LongStatistics) statistics).getMax();
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        minSetter.accept(PageValueConversions.localDateTimeFromEpochMillis(minLong));
                        maxSetter.accept(PageValueConversions.localDateTimeFromEpochMillis(maxLong));
                        return true;
                    case MICROS:
                        minSetter.accept(PageValueConversions.localDateTimeFromEpochMicros(minLong));
                        maxSetter.accept(PageValueConversions.localDateTimeFromEpochMicros(maxLong));
                        return true;
                    case NANOS:
                        minSetter.accept(PageValueConversions.localDateTimeFromEpochNanos(minLong));
                        maxSetter.accept(PageValueConversions.localDateTimeFromEpochNanos(maxLong));
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum values for {@link LocalDate} from given {@code statistics}.
     * <p>
     * LocalDate values can be read from parquet statistics of logical type DATE.
     */
    static boolean getMinMaxForLocalDates(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<LocalDate> minSetter,
            @NotNull final Consumer<LocalDate> maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
            final IntStatistics intStats = (IntStatistics) statistics;
            minSetter.accept(PageValueConversions.localDateFromEpochDay(intStats.getMin()));
            maxSetter.accept(PageValueConversions.localDateFromEpochDay(intStats.getMax()));
            return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum values for {@link LocalTime} from given {@code statistics}.
     * <p>
     * LocalTime values can be read from parquet statistics of logical type TIME.
     */
    static boolean getMinMaxForLocalTimes(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<LocalTime> minSetter,
            @NotNull final Consumer<LocalTime> maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType =
                    (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalType;
            switch (timeLogicalType.getUnit()) {
                case MILLIS: {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(PageValueConversions.localTimeFromMillisOfDay(intStats.getMin()));
                    maxSetter.accept(PageValueConversions.localTimeFromMillisOfDay(intStats.getMax()));
                    return true;
                }
                case MICROS: {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                    final LongStatistics longStats = (LongStatistics) statistics;
                    minSetter.accept(PageValueConversions.localTimeFromMicrosOfDay(longStats.getMin()));
                    maxSetter.accept(PageValueConversions.localTimeFromMicrosOfDay(longStats.getMax()));
                    return true;
                }
                case NANOS: {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                    final LongStatistics longStats = (LongStatistics) statistics;
                    minSetter.accept(PageValueConversions.localTimeFromNanosOfDay(longStats.getMin()));
                    maxSetter.accept(PageValueConversions.localTimeFromNanosOfDay(longStats.getMax()));
                    return true;
                }
            }
        }
        return false;
    }
}
