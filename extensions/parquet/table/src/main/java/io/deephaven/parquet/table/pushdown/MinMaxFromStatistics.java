//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.parquet.base.materializers.IntFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.IntFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.IntFromUnsignedShortMaterializer;
import io.deephaven.parquet.base.materializers.LongFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedByteMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedIntMaterializer;
import io.deephaven.parquet.base.materializers.LongFromUnsignedShortMaterializer;
import io.deephaven.parquet.base.materializers.ShortFromBooleanMaterializer;
import io.deephaven.parquet.base.materializers.ShortFromUnsignedByteMaterializer;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import static io.deephaven.util.type.TypeUtils.getBoxedType;

@InternalUseOnly
public abstract class MinMaxFromStatistics {

    /**
     * Get the (non-NaN) min and max values from the statistics.
     *
     * @param statistics The statistics to analyze
     * @param dhColumnType The expected type of the column in the Deephaven table. This is used to determine how to
     *        interpret the min and max values from the statistics. Also, this will be the type of the returned minimum
     *        and maximum values.
     * @return An {@link Optional} the min and max values from the statistics, or empty if statistics are missing or
     *         unsupported.
     */
    public static Optional<MinMax<?>> get(
            @Nullable final Statistics<?> statistics,
            @NotNull final Class<?> dhColumnType) {
        if (statistics == null || !statistics.hasNonNullValue()) {
            // Cannot determine min/max
            return Optional.empty();
        }
        if (statistics.genericGetMin() == null || statistics.genericGetMax() == null) {
            // Not expected to have null min/max values, but if they are null, we cannot determine min/max
            return Optional.empty();
        }
        final PrimitiveType parquetColType = statistics.type();
        if (parquetColType.columnOrder() != ColumnOrder.typeDefined()) {
            // We only handle typeDefined min/max right now; if new orders get defined in the future, they need to be
            // explicitly handled
            return Optional.empty();
        }
        // First try from logical type, then primitive type, similar to how the Parquet reader does it
        // (see ParquetColumnLocation.makeToPage).
        final LogicalTypeAnnotation logicalType = parquetColType.getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetColType.getPrimitiveTypeName();

        return fromLogicalType(logicalType, statistics, dhColumnType)
                .or(() -> fromPrimitiveType(primitiveTypeName, statistics, dhColumnType));
    }

    private static <T extends Comparable<T>> Optional<MinMax<?>> wrapMinMax(
            @NotNull final T min,
            @NotNull final T max,
            @NotNull final Class<?> dhColumnType) {
        if (min.getClass() != getBoxedType(dhColumnType)) {
            throw new IllegalStateException("Min value type " + min.getClass().getName() + " does not match expected " +
                    "type " + getBoxedType(dhColumnType));
        }
        return Optional.of(MinMax.of(min, max));
    }

    private static Optional<MinMax<?>> fromLogicalType(
            @Nullable final LogicalTypeAnnotation logicalType,
            @NotNull final Statistics<?> statistics,
            @NotNull final Class<?> dhColumnType) {
        if (logicalType != null) {
            return logicalType.accept(new LogicalTypeVisitor(statistics, dhColumnType));
        }
        return Optional.empty();
    }

    private static Optional<MinMax<?>> fromPrimitiveType(
            @NotNull final PrimitiveType.PrimitiveTypeName typeName,
            @NotNull final Statistics<?> statistics,
            @NotNull final Class<?> dhColumnType) {
        switch (typeName) {
            case BOOLEAN:
                final Boolean minBool = (Boolean) statistics.genericGetMin();
                final Boolean maxBool = (Boolean) statistics.genericGetMax();
                if (dhColumnType == Boolean.class || dhColumnType == boolean.class) {
                    return wrapMinMax(minBool, maxBool, dhColumnType);
                } else if (dhColumnType == Short.class || dhColumnType == short.class) {
                    return wrapMinMax(
                            ShortFromBooleanMaterializer.convertValue(minBool),
                            ShortFromBooleanMaterializer.convertValue(maxBool),
                            dhColumnType);
                } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                    return wrapMinMax(
                            IntFromBooleanMaterializer.convertValue(minBool),
                            IntFromBooleanMaterializer.convertValue(maxBool),
                            dhColumnType);
                } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                    return wrapMinMax(
                            LongFromBooleanMaterializer.convertValue(minBool),
                            LongFromBooleanMaterializer.convertValue(maxBool),
                            dhColumnType);
                }
                break;
            case INT32:
                final Integer minInt = (Integer) statistics.genericGetMin();
                final Integer maxInt = (Integer) statistics.genericGetMax();
                if (dhColumnType == Integer.class || dhColumnType == int.class) {
                    return wrapMinMax(minInt, maxInt, dhColumnType);
                } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                    return wrapMinMax(minInt.longValue(), maxInt.longValue(), dhColumnType);
                }
                break;
            case INT64:
                if (dhColumnType == Long.class || dhColumnType == long.class) {
                    return wrapMinMax(
                            (Long) statistics.genericGetMin(),
                            (Long) statistics.genericGetMax(),
                            dhColumnType);
                }
                break;
            case FLOAT:
                final Float minFloat = (Float) statistics.genericGetMin();
                final Float maxFloat = (Float) statistics.genericGetMax();
                if (minFloat.isNaN() || maxFloat.isNaN()) {
                    // NaN is not a valid min/max value and should have been handled automatically by the Builder logic,
                    // so we return empty
                    return Optional.empty();
                }
                if (dhColumnType == Float.class || dhColumnType == float.class) {
                    return wrapMinMax(minFloat, maxFloat, dhColumnType);
                } else if (dhColumnType == Double.class || dhColumnType == double.class) {
                    return wrapMinMax(minFloat.doubleValue(), maxFloat.doubleValue(), dhColumnType);
                }
                break;
            case DOUBLE:
                final Double minDouble = (Double) statistics.genericGetMin();
                final Double maxDouble = (Double) statistics.genericGetMax();
                if (minDouble.isNaN() || maxDouble.isNaN()) {
                    // NaN is not a valid min/max value and should have been handled automatically by the Builder logic,
                    // so we return empty
                    return Optional.empty();
                }
                if (dhColumnType == Double.class || dhColumnType == double.class) {
                    return wrapMinMax(minDouble, maxDouble, dhColumnType);
                }
                break;
            case INT96: // The column-order for INT96 is undefined, so cannot use the statistics.
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
        }
        return Optional.empty();
    }

    private static class LogicalTypeVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<MinMax<?>> {
        final Statistics<?> statistics;
        final Class<?> dhColumnType;

        LogicalTypeVisitor(@NotNull final Statistics<?> statistics, @NotNull final Class<?> dhColumnType) {
            this.statistics = statistics;
            this.dhColumnType = dhColumnType;
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            if (dhColumnType == String.class) {
                return wrapMinMax(statistics.minAsString(), statistics.maxAsString(), dhColumnType);
            }
            return Optional.empty();
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            final long minFromStatistics = (Long) statistics.genericGetMin();
            final long maxFromStatistics = (Long) statistics.genericGetMax();
            if (timestampLogicalType.isAdjustedToUTC() && dhColumnType == Instant.class) {
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochMillisToInstant(minFromStatistics),
                                ParquetPushdownUtils.epochMillisToInstant(maxFromStatistics),
                                dhColumnType);
                    case MICROS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochMicrosToInstant(minFromStatistics),
                                ParquetPushdownUtils.epochMicrosToInstant(maxFromStatistics),
                                dhColumnType);
                    case NANOS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochNanosToInstant(minFromStatistics),
                                ParquetPushdownUtils.epochNanosToInstant(maxFromStatistics),
                                dhColumnType);
                }
            } else if (!timestampLogicalType.isAdjustedToUTC() && dhColumnType == LocalDateTime.class) {
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochMillisToLocalDateTimeUTC(minFromStatistics),
                                ParquetPushdownUtils.epochMillisToLocalDateTimeUTC(maxFromStatistics),
                                dhColumnType);
                    case MICROS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochMicrosToLocalDateTimeUTC(minFromStatistics),
                                ParquetPushdownUtils.epochMicrosToLocalDateTimeUTC(maxFromStatistics),
                                dhColumnType);
                    case NANOS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochNanosToLocalDateTimeUTC(minFromStatistics),
                                ParquetPushdownUtils.epochNanosToLocalDateTimeUTC(maxFromStatistics),
                                dhColumnType);
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            if (intLogicalType.isSigned()) {
                switch (intLogicalType.getBitWidth()) {
                    case 8:
                        final byte minByte = ((Integer) statistics.genericGetMin()).byteValue();
                        final byte maxByte = ((Integer) statistics.genericGetMax()).byteValue();
                        if (dhColumnType == Byte.class || dhColumnType == byte.class) {
                            return wrapMinMax(minByte, maxByte, dhColumnType);
                        } else if (dhColumnType == Short.class || dhColumnType == short.class) {
                            return wrapMinMax((short) minByte, (short) maxByte, dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax((int) minByte, (int) maxByte, dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax((long) minByte, (long) maxByte, dhColumnType);
                        }
                        break;
                    case 16:
                        final short minShort = ((Integer) statistics.genericGetMin()).shortValue();
                        final short maxShort = ((Integer) statistics.genericGetMax()).shortValue();
                        if (dhColumnType == Short.class || dhColumnType == short.class) {
                            return wrapMinMax(minShort, maxShort, dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax((int) minShort, (int) maxShort, dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax((long) minShort, (long) maxShort, dhColumnType);
                        }
                        break;
                    case 32:
                        final int minInt = (Integer) statistics.genericGetMin();
                        final int maxInt = (Integer) statistics.genericGetMax();
                        if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(minInt, maxInt, dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax((long) minInt, (long) maxInt, dhColumnType);
                        }
                        break;
                    case 64:
                        if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    ((Long) statistics.genericGetMin()),
                                    ((Long) statistics.genericGetMax()),
                                    dhColumnType);
                        }
                        break;
                }
            } else {
                switch (intLogicalType.getBitWidth()) {
                    case 8:
                        final byte minByte = ((Integer) statistics.genericGetMin()).byteValue();
                        final byte maxByte = ((Integer) statistics.genericGetMax()).byteValue();
                        if (dhColumnType == Character.class || dhColumnType == char.class) {
                            return wrapMinMax((char) minByte, (char) maxByte, dhColumnType);
                        } else if (dhColumnType == Short.class || dhColumnType == short.class) {
                            return wrapMinMax(
                                    ShortFromUnsignedByteMaterializer.convertValue(minByte),
                                    ShortFromUnsignedByteMaterializer.convertValue(maxByte),
                                    dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntFromUnsignedByteMaterializer.convertValue(minByte),
                                    IntFromUnsignedByteMaterializer.convertValue(maxByte),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromUnsignedByteMaterializer.convertValue(minByte),
                                    LongFromUnsignedByteMaterializer.convertValue(maxByte),
                                    dhColumnType);
                        }
                        break;
                    case 16:
                        final short minShort = ((Integer) statistics.genericGetMin()).shortValue();
                        final short maxShort = ((Integer) statistics.genericGetMax()).shortValue();
                        if (dhColumnType == Character.class || dhColumnType == char.class) {
                            return wrapMinMax((char) minShort, (char) maxShort, dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntFromUnsignedShortMaterializer.convertValue(minShort),
                                    IntFromUnsignedShortMaterializer.convertValue(maxShort),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromUnsignedShortMaterializer.convertValue(minShort),
                                    LongFromUnsignedShortMaterializer.convertValue(maxShort),
                                    dhColumnType);
                        }
                        break;
                    case 32:
                        if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromUnsignedIntMaterializer.convertValue((Integer) statistics.genericGetMin()),
                                    LongFromUnsignedIntMaterializer.convertValue((Integer) statistics.genericGetMax()),
                                    dhColumnType);
                        }
                        break;
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            if (dhColumnType == LocalDate.class) {
                return wrapMinMax(
                        LocalDate.ofEpochDay((Integer) statistics.genericGetMin()),
                        LocalDate.ofEpochDay((Integer) statistics.genericGetMax()),
                        dhColumnType);
            }
            return Optional.empty();
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            if (dhColumnType == LocalTime.class) {
                switch (timeLogicalType.getUnit()) {
                    case MILLIS:
                        return wrapMinMax(
                                ParquetPushdownUtils.millisOfDayToLocalTime((Integer) statistics.genericGetMin()),
                                ParquetPushdownUtils.millisOfDayToLocalTime((Integer) statistics.genericGetMax()),
                                dhColumnType);
                    case MICROS:
                        return wrapMinMax(
                                ParquetPushdownUtils.microsOfDayToLocalTime((Long) statistics.genericGetMin()),
                                ParquetPushdownUtils.microsOfDayToLocalTime((Long) statistics.genericGetMax()),
                                dhColumnType);
                    case NANOS:
                        return wrapMinMax(
                                ParquetPushdownUtils.nanosOfDayToLocalTime((Long) statistics.genericGetMin()),
                                ParquetPushdownUtils.nanosOfDayToLocalTime((Long) statistics.genericGetMax()),
                                dhColumnType);
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            // TODO (DH-19666): Add support for BigDecimal and BigInteger min/max values
            return Optional.empty();
        }
    }
}
