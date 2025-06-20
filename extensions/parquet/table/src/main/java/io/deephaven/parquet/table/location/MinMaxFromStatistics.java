//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.parquet.base.materializers.InstantNanosFromInt96Materializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.InstantNanosFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalDateTimeFromNanosMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromMicrosMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromMillisMaterializer;
import io.deephaven.parquet.base.materializers.LocalTimeFromNanosMaterializer;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Optional;

abstract class MinMaxFromStatistics {

    /**
     * Get the min and max values from the statistics.
     *
     * @param statistics The statistics to analyze
     * @return An {@link Optional} the min and max values from the statistics, or empty if statistics are missing or
     *         unsupported.
     */
    static Optional<MinMax<?>> get(@Nullable final Statistics<?> statistics) {
        if (statistics == null || statistics.isEmpty()) {
            return Optional.empty();
        }
        if (!statistics.hasNonNullValue()) {
            return wrapMinMax((Integer) null, (Integer) null);
        }
        // First try from logical type, then from primitive type
        final LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = statistics.type().getPrimitiveTypeName();
        return fromLogicalType(logicalType, statistics)
                .or(() -> fromPrimitiveType(primitiveTypeName, statistics));
    }

    private static <T extends Comparable<T>> Optional<MinMax<?>> wrapMinMax(final T min, final T max) {
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
                return wrapMinMax(
                        InstantNanosFromInt96Materializer.convertValue(statistics.getMinBytes()),
                        InstantNanosFromInt96Materializer.convertValue(statistics.getMaxBytes()));
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
                                InstantNanosFromMillisMaterializer.convertValue(minFromStatistics),
                                InstantNanosFromMillisMaterializer.convertValue(maxFromStatistics));
                    case MICROS:
                        return wrapMinMax(
                                InstantNanosFromMicrosMaterializer.convertValue(minFromStatistics),
                                InstantNanosFromMicrosMaterializer.convertValue(maxFromStatistics));
                    case NANOS:
                        return wrapMinMax(minFromStatistics, maxFromStatistics);
                    default:
                        throw new IllegalArgumentException("Unsupported unit=" + timestampLogicalType.getUnit());
                }
            }
            switch (timestampLogicalType.getUnit()) {
                case MILLIS:
                    return wrapMinMax(
                            LocalDateTimeFromMillisMaterializer.convertValue(minFromStatistics),
                            LocalDateTimeFromMillisMaterializer.convertValue(maxFromStatistics));
                case MICROS:
                    return wrapMinMax(
                            LocalDateTimeFromMicrosMaterializer.convertValue(minFromStatistics),
                            LocalDateTimeFromMicrosMaterializer.convertValue(maxFromStatistics));
                case NANOS:
                    return wrapMinMax(
                            LocalDateTimeFromNanosMaterializer.convertValue(minFromStatistics),
                            LocalDateTimeFromNanosMaterializer.convertValue(maxFromStatistics));
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
                    LocalDateMaterializer.convertValue((Integer) statistics.genericGetMin()),
                    LocalDateMaterializer.convertValue((Integer) statistics.genericGetMax()));
        }

        @Override
        public Optional<MinMax<?>> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            switch (timeLogicalType.getUnit()) {
                case MILLIS:
                    return wrapMinMax(
                            LocalTimeFromMillisMaterializer.convertValue((Integer) statistics.genericGetMin()),
                            LocalTimeFromMillisMaterializer.convertValue((Integer) statistics.genericGetMax()));
                case MICROS:
                    return wrapMinMax(
                            LocalTimeFromMicrosMaterializer.convertValue((Long) statistics.genericGetMin()),
                            LocalTimeFromMicrosMaterializer.convertValue((Long) statistics.genericGetMax()));
                case NANOS:
                    return wrapMinMax(
                            LocalTimeFromNanosMaterializer.convertValue((Long) statistics.genericGetMin()),
                            LocalTimeFromNanosMaterializer.convertValue((Long) statistics.genericGetMax()));
                default:
                    throw new IllegalArgumentException("Unsupported unit=" + timeLogicalType.getUnit());
            }
        }

        @Override
        public Optional<MinMax<?>> visit(
                final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            // Read the min and max as string and convert them to BigDecimal
            // This is helpful because big decimals can be stored in various formats (int64, binary, etc.), so it's
            // easier to let the internal `Statistics.stringify` handle the conversion
            final String minAsString = statistics.minAsString();
            final String maxAsString = statistics.maxAsString();
            final BigDecimal minValue;
            final BigDecimal maxValue;
            try {
                minValue = new BigDecimal(minAsString);
                maxValue = new BigDecimal(maxAsString);
            } catch (final NumberFormatException exception) {
                // If the min or max cannot be parsed as a BigDecimal, we return empty
                return Optional.empty();
            }
            return wrapMinMax(minValue, maxValue);
        }
    }
}
