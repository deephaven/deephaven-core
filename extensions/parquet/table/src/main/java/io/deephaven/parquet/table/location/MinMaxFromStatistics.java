//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.function.FloatConsumer;
import io.deephaven.engine.primitive.function.ShortConsumer;
import io.deephaven.parquet.base.materializers.ByteMaterializer;
import io.deephaven.parquet.base.materializers.CharMaterializer;
import io.deephaven.parquet.base.materializers.DoubleFromFloatMaterializer;
import io.deephaven.parquet.base.materializers.DoubleMaterializer;
import io.deephaven.parquet.base.materializers.FloatMaterializer;
import io.deephaven.parquet.base.materializers.IntFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.IntFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.IntFromUnsignedShortMaterializer;
import io.deephaven.parquet.base.materializers.IntMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromNanosMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromNanosMaterializer;
import io.deephaven.parquet.base.materializers.LongFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.LongFromIntMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedIntMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedShortMaterializer;
import io.deephaven.parquet.base.materializers.LongMaterializer;
import io.deephaven.parquet.base.materializers.ShortFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.ShortFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.ShortMaterializer;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
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

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MICROS;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MILLIS;


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
                minSetter.accept(ByteMaterializer.convertValue(intStats.getMin()));
                maxSetter.accept(ByteMaterializer.convertValue(intStats.getMax()));
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
                    minSetter.accept(CharMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(CharMaterializer.convertValue(intStats.getMax()));
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
                minSetter.accept(ShortMaterializer.convertValue(intStats.getMin()));
                maxSetter.accept(ShortMaterializer.convertValue(intStats.getMax()));
                return true;
            } else if (!isSigned && bitWidth == 8) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                final IntStatistics intStats = (IntStatistics) statistics;
                minSetter.accept(ShortFromUnsignedByteMaterializer.convertValue(intStats.getMin()));
                maxSetter.accept(ShortFromUnsignedByteMaterializer.convertValue(intStats.getMax()));
                return true;
            }
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            final BooleanStatistics booleanStatistics = (BooleanStatistics) statistics;
            minSetter.accept(ShortFromBooleanMaterializer.convertValue(booleanStatistics.getMin()));
            maxSetter.accept(ShortFromBooleanMaterializer.convertValue(booleanStatistics.getMax()));
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
                minSetter.accept(IntMaterializer.convertValue(intStats.getMin()));
                maxSetter.accept(IntMaterializer.convertValue(intStats.getMax()));
                return true;
            } else if (!isSigned) {
                if (bitWidth == 8) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(IntFromUnsignedByteMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(IntFromUnsignedByteMaterializer.convertValue(intStats.getMax()));
                    return true;
                } else if (bitWidth == 16) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(IntFromUnsignedShortMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(IntFromUnsignedShortMaterializer.convertValue(intStats.getMax()));
                    return true;
                }
            }
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            final BooleanStatistics booleanStats = (BooleanStatistics) statistics;
            minSetter.accept(IntFromBooleanMaterializer.convertValue(booleanStats.getMin()));
            maxSetter.accept(IntFromBooleanMaterializer.convertValue(booleanStats.getMax()));
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
            final IntStatistics intStats = (IntStatistics) statistics;
            minSetter.accept(IntMaterializer.convertValue(intStats.getMin()));
            maxSetter.accept(IntMaterializer.convertValue(intStats.getMax()));
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
                    minSetter.accept(LongFromIntMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(LongFromIntMaterializer.convertValue(intStats.getMax()));
                    return true;
                } else if (bitWidth == 64) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                    final LongStatistics longStats = (LongStatistics) statistics;
                    minSetter.accept(LongMaterializer.convertValue(longStats.getMin()));
                    maxSetter.accept(LongMaterializer.convertValue(longStats.getMax()));
                    return true;
                }
            } else {
                if (bitWidth == 8) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(LongFromUnsignedByteMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(LongFromUnsignedByteMaterializer.convertValue(intStats.getMax()));
                    return true;
                } else if (bitWidth == 16) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(LongFromUnsignedShortMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(LongFromUnsignedShortMaterializer.convertValue(intStats.getMax()));
                    return true;
                } else if (bitWidth == 32) {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT32);
                    final IntStatistics intStats = (IntStatistics) statistics;
                    minSetter.accept(LongFromUnsignedIntMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(LongFromUnsignedIntMaterializer.convertValue(intStats.getMax()));
                    return true;
                }
            }
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
            final BooleanStatistics booleanStats = (BooleanStatistics) statistics;
            minSetter.accept(LongFromBooleanMaterializer.convertValue(booleanStats.getMin()));
            maxSetter.accept(LongFromBooleanMaterializer.convertValue(booleanStats.getMax()));
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
            final IntStatistics intStats = (IntStatistics) statistics;
            minSetter.accept(LongFromIntMaterializer.convertValue(intStats.getMin()));
            maxSetter.accept(LongFromIntMaterializer.convertValue(intStats.getMax()));
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
            final LongStatistics longStats = (LongStatistics) statistics;
            minSetter.accept(LongMaterializer.convertValue(longStats.getMin()));
            maxSetter.accept(LongMaterializer.convertValue(longStats.getMax()));
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
            minSetter.accept(FloatMaterializer.convertValue(minFloat));
            maxSetter.accept(FloatMaterializer.convertValue(maxFloat));
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
            minSetter.accept(DoubleFromFloatMaterializer.convertValue(minFloat));
            maxSetter.accept(DoubleFromFloatMaterializer.convertValue(maxFloat));
            return true;
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE) {
            final double minDouble = ((DoubleStatistics) statistics).getMin();
            final double maxDouble = ((DoubleStatistics) statistics).getMax();
            if (Double.isNaN(minDouble) || Double.isNaN(maxDouble)) {
                // NaN is not a valid min/max value and should have been handled automatically by the Builder logic,
                // so we return empty
                return false;
            }
            minSetter.accept(DoubleMaterializer.convertValue(minDouble));
            maxSetter.accept(DoubleMaterializer.convertValue(maxDouble));
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
     * Instant values can be read from parquet statistics of logical type TIMESTAMP (when adjusted to UTC). Each unit
     * converts straight to an {@link Instant}, never by way of epoch nanoseconds: {@link Instant} covers every value a
     * MILLIS or MICROS statistic can hold, whereas epoch nanoseconds stop short of the year 2262 and would reject, for
     * example, the common {@code 9999-12-31} sentinel.
     */
    static boolean getMinMaxForInstants(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<Instant> minSetter,
            @NotNull final Consumer<Instant> maxSetter) {
        final LogicalTypeAnnotation.TimeUnit unit = utcTimestampUnit(statistics);
        if (unit == null) {
            return false;
        }
        final long minLong = ((LongStatistics) statistics).getMin();
        final long maxLong = ((LongStatistics) statistics).getMax();
        switch (unit) {
            case MILLIS:
                minSetter.accept(Instant.ofEpochMilli(minLong));
                maxSetter.accept(Instant.ofEpochMilli(maxLong));
                return true;
            case MICROS:
                minSetter.accept(epochMicrosToInstant(minLong));
                maxSetter.accept(epochMicrosToInstant(maxLong));
                return true;
            case NANOS:
                minSetter.accept(ParquetPushdownUtils.epochNanosToInstant(minLong));
                maxSetter.accept(ParquetPushdownUtils.epochNanosToInstant(maxLong));
                return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum values for instants from the given {@code statistics}, as epoch
     * nanoseconds -- the representation Deephaven stores an {@link Instant} column in.
     * <p>
     * NANOS statistics are already epoch nanoseconds and pass through unchanged; going by way of an {@link Instant}
     * would fail for the top of the {@code long} range, which {@link DateTimeUtils#epochNanos} cannot convert back.
     * <p>
     * MILLIS and MICROS statistics beyond the range of epoch nanoseconds (roughly the years 1677 to 2262) saturate
     * instead of failing, so the other extreme goes on pruning. A saturated extreme is not a value the row group holds,
     * so it is chosen such that wherever it compares differently from the true value, the row group is kept rather than
     * excluded.
     * <ul>
     * <li>After the range, either extreme becomes {@link Long#MAX_VALUE}. It compares just as the true value would with
     * everything but itself, and no filter can name it: {@link DateTimeUtils#epochNanos} rejects the {@link Instant} it
     * stands for.</li>
     * <li>Before the range, no single value will do. Deephaven orders null below every value, so the true value lies
     * above {@code NULL_LONG} and below {@link QueryConstants#MIN_LONG}, two adjacent {@code long}s. A minimum becomes
     * {@code NULL_LONG} ({@link Long#MIN_VALUE}) and a maximum {@code MIN_LONG}, straddling it, so a row group of such
     * values never looks like nothing but nulls, like a single value, or like it holds nothing before {@code MIN_LONG}.
     * The price is that an is-null filter cannot exclude a row group whose minimum is before the range; it reads it
     * instead, and the reader rejects the value.</li>
     * </ul>
     */
    static boolean getMinMaxForInstantNanos(
            @NotNull final Statistics<?> statistics,
            @NotNull final LongConsumer minSetter,
            @NotNull final LongConsumer maxSetter) {
        final LogicalTypeAnnotation.TimeUnit unit = utcTimestampUnit(statistics);
        if (unit == null) {
            return false;
        }
        final long minLong = ((LongStatistics) statistics).getMin();
        final long maxLong = ((LongStatistics) statistics).getMax();
        final long maxConvertible;
        final long nanosPerUnit;
        switch (unit) {
            case MILLIS:
                maxConvertible = MAX_CONVERTIBLE_MILLIS;
                nanosPerUnit = DateTimeUtils.MILLI;
                break;
            case MICROS:
                maxConvertible = MAX_CONVERTIBLE_MICROS;
                nanosPerUnit = DateTimeUtils.MICRO;
                break;
            case NANOS:
                minSetter.accept(minLong);
                maxSetter.accept(maxLong);
                return true;
            default:
                return false;
        }
        // A statistic too early for epoch nanoseconds (before 1677) has no long of its own: as a minimum it becomes
        // NULL_LONG and as a maximum MIN_LONG, the two longs either side of where Deephaven would order it. See above.
        minSetter.accept(saturatingToNanos(minLong, maxConvertible, nanosPerUnit, Long.MIN_VALUE));
        maxSetter.accept(saturatingToNanos(maxLong, maxConvertible, nanosPerUnit, QueryConstants.MIN_LONG));
        return true;
    }

    /**
     * The unit of a TIMESTAMP column adjusted to UTC -- the columns read as {@link Instant} -- or {@code null} for any
     * other column.
     */
    private static LogicalTypeAnnotation.TimeUnit utcTimestampUnit(@NotNull final Statistics<?> statistics) {
        final LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
            if (timestampLogicalType.isAdjustedToUTC()) {
                verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                return timestampLogicalType.getUnit();
            }
        }
        return null;
    }

    private static Instant epochMicrosToInstant(final long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    /**
     * {@code value * nanosPerUnit}; or, beyond {@code maxConvertible} -- the same limit the reader's materializers
     * enforce -- {@link Long#MAX_VALUE} above and {@code belowRange} below. See {@link #getMinMaxForInstantNanos} for
     * what {@code belowRange} must be for each extreme.
     */
    private static long saturatingToNanos(
            final long value, final long maxConvertible, final long nanosPerUnit, final long belowRange) {
        if (value > maxConvertible) {
            return Long.MAX_VALUE;
        }
        if (value < -maxConvertible) {
            return belowRange;
        }
        return value * nanosPerUnit;
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
                        minSetter.accept(LocalDateTimeFromMillisMaterializer.convertValue(minLong));
                        maxSetter.accept(LocalDateTimeFromMillisMaterializer.convertValue(maxLong));
                        return true;
                    case MICROS:
                        minSetter.accept(LocalDateTimeFromMicrosMaterializer.convertValue(minLong));
                        maxSetter.accept(LocalDateTimeFromMicrosMaterializer.convertValue(maxLong));
                        return true;
                    case NANOS:
                        minSetter.accept(LocalDateTimeFromNanosMaterializer.convertValue(minLong));
                        maxSetter.accept(LocalDateTimeFromNanosMaterializer.convertValue(maxLong));
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
            minSetter.accept(LocalDateMaterializer.convertValue(intStats.getMin()));
            maxSetter.accept(LocalDateMaterializer.convertValue(intStats.getMax()));
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
                    minSetter.accept(LocalTimeFromMillisMaterializer.convertValue(intStats.getMin()));
                    maxSetter.accept(LocalTimeFromMillisMaterializer.convertValue(intStats.getMax()));
                    return true;
                }
                case MICROS: {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                    final LongStatistics longStats = (LongStatistics) statistics;
                    minSetter.accept(LocalTimeFromMicrosMaterializer.convertValue(longStats.getMin()));
                    maxSetter.accept(LocalTimeFromMicrosMaterializer.convertValue(longStats.getMax()));
                    return true;
                }
                case NANOS: {
                    verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.INT64);
                    final LongStatistics longStats = (LongStatistics) statistics;
                    minSetter.accept(LocalTimeFromNanosMaterializer.convertValue(longStats.getMin()));
                    maxSetter.accept(LocalTimeFromNanosMaterializer.convertValue(longStats.getMax()));
                    return true;
                }
            }
        }
        return false;
    }
}
