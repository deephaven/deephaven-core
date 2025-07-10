//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

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
                    return wrapMinMax(
                            IntMaterializer.convertValue(minInt),
                            IntMaterializer.convertValue(maxInt),
                            dhColumnType);
                } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                    return wrapMinMax(
                            LongFromIntMaterializer.convertValue(minInt),
                            LongFromIntMaterializer.convertValue(maxInt),
                            dhColumnType);
                }
                break;
            case INT64:
                if (dhColumnType == Long.class || dhColumnType == long.class) {
                    return wrapMinMax(
                            LongMaterializer.convertValue((Long) statistics.genericGetMin()),
                            LongMaterializer.convertValue((Long) statistics.genericGetMax()),
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
                    return wrapMinMax(
                            FloatMaterializer.convertValue(minFloat),
                            FloatMaterializer.convertValue(maxFloat),
                            dhColumnType);
                } else if (dhColumnType == Double.class || dhColumnType == double.class) {
                    return wrapMinMax(
                            DoubleFromFloatMaterializer.convertValue(minFloat),
                            DoubleFromFloatMaterializer.convertValue(maxFloat),
                            dhColumnType);
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
                    return wrapMinMax(
                            DoubleMaterializer.convertValue(minDouble),
                            DoubleMaterializer.convertValue(maxDouble),
                            dhColumnType);
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
                                ParquetPushdownUtils.epochNanosToInstant(
                                        InstantNanosFromMillisMaterializer.convertValue(minFromStatistics)),
                                ParquetPushdownUtils.epochNanosToInstant(
                                        InstantNanosFromMillisMaterializer.convertValue(maxFromStatistics)),
                                dhColumnType);
                    case MICROS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochNanosToInstant(
                                        InstantNanosFromMicrosMaterializer.convertValue(minFromStatistics)),
                                ParquetPushdownUtils.epochNanosToInstant(
                                        InstantNanosFromMicrosMaterializer.convertValue(maxFromStatistics)),
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
                                LocalDateTimeFromMillisMaterializer.convertValue(minFromStatistics),
                                LocalDateTimeFromMillisMaterializer.convertValue(maxFromStatistics),
                                dhColumnType);
                    case MICROS:
                        return wrapMinMax(
                                LocalDateTimeFromMicrosMaterializer.convertValue(minFromStatistics),
                                LocalDateTimeFromMicrosMaterializer.convertValue(maxFromStatistics),
                                dhColumnType);
                    case NANOS:
                        return wrapMinMax(
                                LocalDateTimeFromNanosMaterializer.convertValue(minFromStatistics),
                                LocalDateTimeFromNanosMaterializer.convertValue(maxFromStatistics),
                                dhColumnType);
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            if (intLogicalType.isSigned()) {
                switch (intLogicalType.getBitWidth()) {
                    case 8: {
                        final int min = ((Integer) statistics.genericGetMin());
                        final int max = ((Integer) statistics.genericGetMax());
                        if (dhColumnType == Byte.class || dhColumnType == byte.class) {
                            return wrapMinMax(
                                    ByteMaterializer.convertValue(min),
                                    ByteMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Short.class || dhColumnType == short.class) {
                            return wrapMinMax(
                                    ShortMaterializer.convertValue(min),
                                    ShortMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntMaterializer.convertValue(min),
                                    IntMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromIntMaterializer.convertValue(min),
                                    LongFromIntMaterializer.convertValue(max),
                                    dhColumnType);
                        }
                    }
                        break;
                    case 16: {
                        final int min = ((Integer) statistics.genericGetMin());
                        final int max = ((Integer) statistics.genericGetMax());
                        if (dhColumnType == Short.class || dhColumnType == short.class) {
                            return wrapMinMax(
                                    ShortMaterializer.convertValue(min),
                                    ShortMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntMaterializer.convertValue(min),
                                    IntMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromIntMaterializer.convertValue(min),
                                    LongFromIntMaterializer.convertValue(max),
                                    dhColumnType);
                        }
                    }
                        break;
                    case 32: {
                        final int min = ((Integer) statistics.genericGetMin());
                        final int max = ((Integer) statistics.genericGetMax());
                        if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntMaterializer.convertValue(min),
                                    IntMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromIntMaterializer.convertValue(min),
                                    LongFromIntMaterializer.convertValue(max),
                                    dhColumnType);
                        }
                    }
                        break;
                    case 64:
                        if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongMaterializer.convertValue((Long) statistics.genericGetMin()),
                                    LongMaterializer.convertValue((Long) statistics.genericGetMax()),
                                    dhColumnType);
                        }
                        break;
                }
            } else {
                switch (intLogicalType.getBitWidth()) {
                    case 8: {
                        final int min = ((Integer) statistics.genericGetMin());
                        final int max = ((Integer) statistics.genericGetMax());
                        if (dhColumnType == Character.class || dhColumnType == char.class) {
                            return wrapMinMax(
                                    CharMaterializer.convertValue(min),
                                    CharMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Short.class || dhColumnType == short.class) {
                            return wrapMinMax(
                                    ShortFromUnsignedByteMaterializer.convertValue(min),
                                    ShortFromUnsignedByteMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntFromUnsignedByteMaterializer.convertValue(min),
                                    IntFromUnsignedByteMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromUnsignedByteMaterializer.convertValue(min),
                                    LongFromUnsignedByteMaterializer.convertValue(max),
                                    dhColumnType);
                        }
                    }
                        break;
                    case 16: {
                        final int min = ((Integer) statistics.genericGetMin());
                        final int max = ((Integer) statistics.genericGetMax());
                        if (dhColumnType == Character.class || dhColumnType == char.class) {
                            return wrapMinMax(
                                    CharMaterializer.convertValue(min),
                                    CharMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Integer.class || dhColumnType == int.class) {
                            return wrapMinMax(
                                    IntFromUnsignedShortMaterializer.convertValue(min),
                                    IntFromUnsignedShortMaterializer.convertValue(max),
                                    dhColumnType);
                        } else if (dhColumnType == Long.class || dhColumnType == long.class) {
                            return wrapMinMax(
                                    LongFromUnsignedShortMaterializer.convertValue(min),
                                    LongFromUnsignedShortMaterializer.convertValue(max),
                                    dhColumnType);
                        }
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
                        LocalDateMaterializer.convertValue((Integer) statistics.genericGetMin()),
                        LocalDateMaterializer.convertValue((Integer) statistics.genericGetMax()),
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
                                LocalTimeFromMillisMaterializer.convertValue((Integer) statistics.genericGetMin()),
                                LocalTimeFromMillisMaterializer.convertValue((Integer) statistics.genericGetMax()),
                                dhColumnType);
                    case MICROS:
                        return wrapMinMax(
                                LocalTimeFromMicrosMaterializer.convertValue((Long) statistics.genericGetMin()),
                                LocalTimeFromMicrosMaterializer.convertValue((Long) statistics.genericGetMax()),
                                dhColumnType);
                    case NANOS:
                        return wrapMinMax(
                                LocalTimeFromNanosMaterializer.convertValue((Long) statistics.genericGetMin()),
                                LocalTimeFromNanosMaterializer.convertValue((Long) statistics.genericGetMax()),
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
