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

import java.util.Optional;

abstract class MinMaxFromStatistics {

    /**
     * Get the min and max values from the statistics.
     *
     * @param statistics The statistics to analyze
     * @return An {@link Optional} the min and max values from the statistics, or empty if statistics are missing or
     *         unsupported.
     */
    static Optional<MinMax> get(@Nullable final Statistics<?> statistics) {
        if (statistics == null || statistics.isEmpty()) {
            return Optional.empty();
        }
        if (!statistics.hasNonNullValue()) {
            return wrapMinMax(null, null);
        }
        // First try from logical type, then from primitive type
        final LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = statistics.type().getPrimitiveTypeName();
        return fromLogicalType(logicalType, statistics)
                .or(() -> fromPrimitiveType(primitiveTypeName, statistics));
    }

    private static Optional<MinMax> wrapMinMax(final Object min, final Object max) {
        return Optional.of(MinMax.of(min, max));
    }

    private static Optional<MinMax> fromLogicalType(
            @Nullable final LogicalTypeAnnotation logicalType,
            @NotNull final Statistics<?> statistics) {
        if (logicalType != null) {
            return logicalType.accept(new LogicalTypeVisitor(statistics));
        }
        return Optional.empty();
    }

    private static Optional<MinMax> fromPrimitiveType(
            @NotNull final PrimitiveType.PrimitiveTypeName typeName,
            @NotNull final Statistics<?> statistics) {
        switch (typeName) {
            case BOOLEAN:
            case INT32:
            case INT64:
            case DOUBLE:
            case FLOAT:
                // For primitive types, we can use the generic min and max values directly
                return wrapMinMax(statistics.genericGetMin(), statistics.genericGetMax());
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

    private static class LogicalTypeVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<MinMax> {
        final Statistics<?> statistics;

        LogicalTypeVisitor(@NotNull final Statistics<?> statistics) {
            this.statistics = statistics;
        }

        @Override
        public Optional<MinMax> visit(
                final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            // TODO Handle string types
            return Optional.empty();
        }

        @Override
        public Optional<MinMax> visit(
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
        public Optional<MinMax> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            final int minFromStatistics = (Integer) statistics.genericGetMin();
            final int maxFromStatistics = (Integer) statistics.genericGetMax();
            if (intLogicalType.isSigned()) {
                return wrapMinMax(minFromStatistics, maxFromStatistics);
            }
            return wrapMinMax(Integer.toUnsignedLong(minFromStatistics), Integer.toUnsignedLong(maxFromStatistics));
        }

        @Override
        public Optional<MinMax> visit(final LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return wrapMinMax(
                    LocalDateMaterializer.convertValue((Integer) statistics.genericGetMin()),
                    LocalDateMaterializer.convertValue((Integer) statistics.genericGetMax()));
        }

        @Override
        public Optional<MinMax> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
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
        public Optional<MinMax> visit(
                final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            // TODO: Handle decimal types
            return Optional.empty();
        }
    }
}
