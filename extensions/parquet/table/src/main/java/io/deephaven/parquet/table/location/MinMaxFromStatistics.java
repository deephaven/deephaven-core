//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
import io.deephaven.parquet.base.materializers.InstantNanosFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMillisMaterializer;
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
        if (!statistics.hasNonNullValue()) {
            throw new IllegalStateException("Statistics must have a non-null value");
        }
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

    static boolean getMinMaxForComparable(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<Comparable<?>> minSetter,
            @NotNull final Consumer<Comparable<?>> maxSetter,
            final Class<?> columnType) {
        if (columnType == String.class) {
            return getMinMaxForStrings(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == Instant.class) {
            return getMinMaxForInstants(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == LocalDateTime.class) {
            return getMinMaxForLocalDateTimes(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == LocalDate.class) {
            return getMinMaxForLocalDates(statistics, minSetter::accept, maxSetter::accept);
        } else if (columnType == LocalTime.class) {
            return getMinMaxForLocalTimes(statistics, minSetter::accept, maxSetter::accept);
        }
        // TODO (DH-19666): Add support for more types like BigDecimal and BigInteger min/max values
        return false;
    }

    /**
     * Attempts to retrieve the minimum and maximum string from the given {@code statistics}.
     */
    static boolean getMinMaxForStrings(
            @NotNull final Statistics<?> statistics,
            @NotNull final Consumer<String> minSetter,
            @NotNull final Consumer<String> maxSetter) {
        final PrimitiveType parquetColType = statistics.type();
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            verifyPrimitive(statistics, PrimitiveType.PrimitiveTypeName.BINARY);
            final String minString = statistics.minAsString();
            final String maxString = statistics.maxAsString();
            minSetter.accept(minString);
            maxSetter.accept(maxString);
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
                                InstantNanosFromMillisMaterializer.convertValue(minLong)));
                        maxSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                InstantNanosFromMillisMaterializer.convertValue(maxLong)));
                        return true;
                    case MICROS:
                        minSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                InstantNanosFromMicrosMaterializer.convertValue(minLong)));
                        maxSetter.accept(ParquetPushdownUtils.epochNanosToInstant(
                                InstantNanosFromMicrosMaterializer.convertValue(maxLong)));
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
