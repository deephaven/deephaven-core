//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.parquet.base.materializers.InstantNanosFromInt96Materializer;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.time.LocalDate;
import java.util.Optional;

@VisibleForTesting
public abstract class MinMaxFromStatistics {

    /**
     * Get the min and max values from the statistics.
     * <p>
     * This class assumes that the statistics do not have any {@code NaN} values because {@code NaN} values are
     * automatically handled for floats and doubles by {@link Statistics.Builder}.
     *
     * @param statistics The statistics to analyze
     * @return An {@link Optional} the min and max values from the statistics, or empty if statistics are missing or
     *         unsupported.
     */
    // TODO Add unit tests for sanitization of statistics done by Statistics.Builder
    public static Optional<MinMax<?>> get(@Nullable final Statistics<?> statistics) {
        if (statistics == null || !statistics.hasNonNullValue()) {
            // Cannot determine min/max
            return Optional.empty();
        }
        if (statistics.genericGetMin() == null || statistics.genericGetMax() == null) {
            // Not expected to have null min/max values, but if they are null, we cannot determine min/max
            return Optional.empty();
        }
        // First try from logical type, then from primitive type
        final LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = statistics.type().getPrimitiveTypeName();
        return fromLogicalType(logicalType, statistics)
                .or(() -> fromPrimitiveType(primitiveTypeName, statistics));
    }

    private static <T extends Comparable<T>> Optional<MinMax<?>> wrapMinMax(
            @NotNull final T min,
            @NotNull final T max) {
        return Optional.of(MinMax.of(min, max));
    }

    private static Optional<MinMax<?>> fromLogicalType(
            @Nullable final LogicalTypeAnnotation logicalType,
            @NotNull final Statistics<?> statistics) {
        if (logicalType != null) {
            return logicalType.accept(new LogicalTypeVisitor(statistics));
        }
        return Optional.empty();
    }

    private static Optional<MinMax<?>> fromPrimitiveType(
            @NotNull final PrimitiveType.PrimitiveTypeName typeName,
            @NotNull final Statistics<?> statistics) {
        switch (typeName) {
            case BOOLEAN:
                return wrapMinMax((Boolean) statistics.genericGetMin(), (Boolean) statistics.genericGetMax());
            case INT32:
                return wrapMinMax((Integer) statistics.genericGetMin(), (Integer) statistics.genericGetMax());
            case INT64:
                return wrapMinMax((Long) statistics.genericGetMin(), (Long) statistics.genericGetMax());
            case DOUBLE:
                return wrapMinMax((Double) statistics.genericGetMin(), (Double) statistics.genericGetMax());
            case FLOAT:
                return wrapMinMax((Float) statistics.genericGetMin(), (Float) statistics.genericGetMax());
            case INT96:
                final long minInstantNanos = InstantNanosFromInt96Materializer.convertValue(statistics.getMinBytes());
                final long maxInstantNanos = InstantNanosFromInt96Materializer.convertValue(statistics.getMaxBytes());
                return wrapMinMax(
                        ParquetPushdownUtils.epochNanosToInstant(minInstantNanos),
                        ParquetPushdownUtils.epochNanosToInstant(maxInstantNanos));
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            default:
                return Optional.empty();
        }
    }

    private static class LogicalTypeVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<MinMax<?>> {
        final Statistics<?> statistics;

        LogicalTypeVisitor(@NotNull final Statistics<?> statistics) {
            this.statistics = statistics;
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            return wrapMinMax(statistics.minAsString(), statistics.maxAsString());
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            final long minFromStatistics = (Long) statistics.genericGetMin();
            final long maxFromStatistics = (Long) statistics.genericGetMax();
            if (timestampLogicalType.isAdjustedToUTC()) {
                switch (timestampLogicalType.getUnit()) {
                    case MILLIS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochMillisToInstant(minFromStatistics),
                                ParquetPushdownUtils.epochMillisToInstant(maxFromStatistics));
                    case MICROS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochMicrosToInstant(minFromStatistics),
                                ParquetPushdownUtils.epochMicrosToInstant(maxFromStatistics));
                    case NANOS:
                        return wrapMinMax(
                                ParquetPushdownUtils.epochNanosToInstant(minFromStatistics),
                                ParquetPushdownUtils.epochNanosToInstant(maxFromStatistics));
                    default:
                        throw new IllegalArgumentException("Unsupported unit=" + timestampLogicalType.getUnit());
                }
            }
            switch (timestampLogicalType.getUnit()) {
                case MILLIS:
                    return wrapMinMax(
                            ParquetPushdownUtils.epochMillisToLocalDateTimeUTC(minFromStatistics),
                            ParquetPushdownUtils.epochMillisToLocalDateTimeUTC(maxFromStatistics));
                case MICROS:
                    return wrapMinMax(
                            ParquetPushdownUtils.epochMicrosToLocalDateTimeUTC(minFromStatistics),
                            ParquetPushdownUtils.epochMicrosToLocalDateTimeUTC(maxFromStatistics));
                case NANOS:
                    return wrapMinMax(
                            ParquetPushdownUtils.epochNanosToLocalDateTimeUTC(minFromStatistics),
                            ParquetPushdownUtils.epochNanosToLocalDateTimeUTC(maxFromStatistics));
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + timestampLogicalType.getUnit());
            }
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            final int minFromStatistics = (Integer) statistics.genericGetMin();
            final int maxFromStatistics = (Integer) statistics.genericGetMax();
            if (intLogicalType.isSigned()) {
                return wrapMinMax(minFromStatistics, maxFromStatistics);
            }
            return wrapMinMax(Integer.toUnsignedLong(minFromStatistics), Integer.toUnsignedLong(maxFromStatistics));
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return wrapMinMax(
                    LocalDate.ofEpochDay((Integer) statistics.genericGetMin()),
                    LocalDate.ofEpochDay((Integer) statistics.genericGetMax()));
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            switch (timeLogicalType.getUnit()) {
                case MILLIS:
                    return wrapMinMax(
                            ParquetPushdownUtils.millisOfDayToLocalTime((Integer) statistics.genericGetMin()),
                            ParquetPushdownUtils.millisOfDayToLocalTime((Integer) statistics.genericGetMax()));
                case MICROS:
                    return wrapMinMax(
                            ParquetPushdownUtils.microsOfDayToLocalTime((Long) statistics.genericGetMin()),
                            ParquetPushdownUtils.microsOfDayToLocalTime((Long) statistics.genericGetMax()));
                case NANOS:
                    return wrapMinMax(
                            ParquetPushdownUtils.nanosOfDayToLocalTime((Long) statistics.genericGetMin()),
                            ParquetPushdownUtils.nanosOfDayToLocalTime((Long) statistics.genericGetMax()));
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + timeLogicalType.getUnit());
            }
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            // TODO (DH-19666): Decimal logical type vales can be stored in various formats (int64, binary, etc.) and
            // can be read as both BigDecimal and BigInteger. So the right way to handle them is to get the column type
            // from the table definition, read the min and max from the statistics as strings, and then convert these
            // strings to the type specified in the table definition.
            return Optional.empty();
        }
    }
}
