package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecApproximatePercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecCountDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFormula;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecMedian;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecPercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSorted;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSortedColumn;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecTDigest;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecWeighted;

class AggSpecAdapter {
    public static void validate(io.deephaven.proto.backplane.grpc.AggSpec spec) {
        // It's a bit unfortunate that generated protobuf objects don't have the names as constants (like it does with
        // field numbers). For example, AggSpec.TYPE_NAME.
        GrpcErrorHelper.checkHasOneOf(spec, "type");
    }

    public static AggSpec adapt(io.deephaven.proto.backplane.grpc.AggSpec spec) {
        switch (spec.getTypeCase()) {
            case ABS_SUM:
                return AggSpec.absSum();
            case APPROXIMATE_PERCENTILE:
                return adapt(spec.getApproximatePercentile());
            case AVG:
                return AggSpec.avg();
            case COUNT_DISTINCT:
                return adapt(spec.getCountDistinct());
            case DISTINCT:
                return adapt(spec.getDistinct());
            case FIRST:
                return AggSpec.first();
            case FORMULA:
                return adapt(spec.getFormula());
            case FREEZE:
                return AggSpec.freeze();
            case GROUP:
                return AggSpec.group();
            case LAST:
                return AggSpec.last();
            case MAX:
                return AggSpec.max();
            case MEDIAN:
                return adapt(spec.getMedian());
            case MIN:
                return AggSpec.min();
            case PERCENTILE:
                return adapt(spec.getPercentile());
            case SORTED_FIRST:
                return adaptFirst(spec.getSortedFirst());
            case SORTED_LAST:
                return adaptLast(spec.getSortedLast());
            case STD:
                return AggSpec.std();
            case SUM:
                return AggSpec.sum();
            case T_DIGEST:
                return adapt(spec.getTDigest());
            case UNIQUE:
                return adapt(spec.getUnique());
            case WEIGHTED_AVG:
                return adaptWeightedAvg(spec.getWeightedAvg());
            case WEIGHTED_SUM:
                return adaptWeightedSum(spec.getWeightedSum());
            case VAR:
                return AggSpec.var();
            case TYPE_NOT_SET:
                // Note: we don't expect this case to be hit - it should be noticed earlier, and can provide a better
                // error message via validate.
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "AggSpec type not set");
            default:
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL,
                        String.format("Server is missing AggSpec case %s", spec.getTypeCase()));
        }
    }

    private static io.deephaven.api.agg.spec.AggSpecApproximatePercentile adapt(
            AggSpecApproximatePercentile percentile) {
        return percentile.hasCompression()
                ? AggSpec.approximatePercentile(percentile.getPercentile(), percentile.getCompression())
                : AggSpec.approximatePercentile(percentile.getPercentile());
    }

    private static io.deephaven.api.agg.spec.AggSpecCountDistinct adapt(AggSpecCountDistinct countDistinct) {
        return countDistinct.hasCountNulls() ? AggSpec.countDistinct(countDistinct.getCountNulls())
                : AggSpec.countDistinct();
    }

    private static io.deephaven.api.agg.spec.AggSpecDistinct adapt(AggSpecDistinct distinct) {
        return distinct.hasIncludeNulls() ? AggSpec.distinct(distinct.getIncludeNulls()) : AggSpec.distinct();
    }

    private static io.deephaven.api.agg.spec.AggSpecFormula adapt(AggSpecFormula formula) {
        return formula.hasParamToken() ? AggSpec.formula(formula.getFormula(), formula.getParamToken())
                : AggSpec.formula(formula.getFormula());
    }

    private static io.deephaven.api.agg.spec.AggSpecMedian adapt(AggSpecMedian median) {
        return median.hasAverageEvenlyDivided() ? AggSpec.median(median.getAverageEvenlyDivided()) : AggSpec.median();
    }

    private static io.deephaven.api.agg.spec.AggSpecPercentile adapt(AggSpecPercentile percentile) {
        return percentile.hasAverageEvenlyDivided()
                ? AggSpec.percentile(percentile.getPercentile(), percentile.getAverageEvenlyDivided())
                : AggSpec.percentile(percentile.getPercentile());
    }

    private static SortColumn adapt(AggSpecSortedColumn sortedColumn) {
        return SortColumn.asc(ColumnName.of(sortedColumn.getColumnName()));
    }

    private static AggSpecSortedFirst adaptFirst(AggSpecSorted sorted) {
        AggSpecSortedFirst.Builder builder = AggSpecSortedFirst.builder();
        for (AggSpecSortedColumn sortedColumn : sorted.getColumnsList()) {
            builder.addColumns(adapt(sortedColumn));
        }
        return builder.build();
    }

    private static AggSpecSortedLast adaptLast(AggSpecSorted sorted) {
        AggSpecSortedLast.Builder builder = AggSpecSortedLast.builder();
        for (AggSpecSortedColumn sortDescriptor : sorted.getColumnsList()) {
            builder.addColumns(adapt(sortDescriptor));
        }
        return builder.build();
    }

    private static io.deephaven.api.agg.spec.AggSpecTDigest adapt(AggSpecTDigest tDigest) {
        return tDigest.hasCompression() ? AggSpec.tDigest(tDigest.getCompression()) : AggSpec.tDigest();
    }

    private static io.deephaven.api.agg.spec.AggSpecUnique adapt(AggSpecUnique unique) {
        Boolean includeNull = unique.hasIncludeNulls() ? unique.getIncludeNulls() : null;
        Object nonUniqueSentinel = unique.hasNonUniqueSentinel() ? adapt(unique.getNonUniqueSentinel()) : null;
        io.deephaven.api.agg.spec.AggSpecUnique.Builder builder = io.deephaven.api.agg.spec.AggSpecUnique.builder();
        if (includeNull != null) {
            builder.includeNulls(includeNull);
        }
        if (nonUniqueSentinel != null) {
            builder.nonUniqueSentinel(nonUniqueSentinel);
        }
        return builder.build();
    }

    private static Object adapt(AggSpecNonUniqueSentinel nonUniqueSentinel) {
        switch (nonUniqueSentinel.getTypeCase()) {
            case STRING_VALUE:
                return nonUniqueSentinel.getStringValue();
            case INT_VALUE:
                return nonUniqueSentinel.getIntValue();
            case LONG_VALUE:
                return nonUniqueSentinel.getLongValue();
            case FLOAT_VALUE:
                return nonUniqueSentinel.getFloatValue();
            case DOUBLE_VALUE:
                return nonUniqueSentinel.getDoubleValue();
            case BOOL_VALUE:
                return nonUniqueSentinel.getBoolValue();
            case TYPE_NOT_SET:
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "AggSpecNonUniqueSentinel type not set");
            default:
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL, String
                        .format("Server is missing AggSpecNonUniqueSentinel case %s", nonUniqueSentinel.getTypeCase()));
        }
    }

    private static AggSpecWAvg adaptWeightedAvg(AggSpecWeighted weighted) {
        return AggSpec.wavg(weighted.getWeightColumn());
    }

    private static AggSpecWSum adaptWeightedSum(AggSpecWeighted weighted) {
        return AggSpec.wsum(weighted.getWeightColumn());
    }
}
