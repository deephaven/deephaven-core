package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.validation.ColumnExpressionValidator;
import io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.api.agg.Aggregation.*;

@Singleton
public class ComboAggregateGrpcImpl extends GrpcTableOperation<ComboAggregateRequest> {

    @Inject
    public ComboAggregateGrpcImpl() {
        super(BatchTableRequest.Operation::getComboAggregate, ComboAggregateRequest::getResultId,
                ComboAggregateRequest::getSourceId);
    }

    @Override
    public void validateRequest(ComboAggregateRequest request) throws StatusRuntimeException {
        if (request.getAggregatesCount() == 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "ComboAggregateRequest incorrectly has zero aggregates provided");
        }
        if (isSimpleAggregation(request)) {
            // this is a simple aggregation, make sure the user didn't mistakenly set extra properties
            // which would suggest they meant to set force_combo=true
            ComboAggregateRequest.Aggregate aggregate = request.getAggregates(0);
            if (aggregate.getMatchPairsCount() != 0) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "force_combo is false and only one aggregate provided, but match_pairs is specified");
            }
            if (aggregate.getPercentile() != 0) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "force_combo is false and only one aggregate provided, but percentile is specified");
            }
            if (aggregate.getAvgMedian()) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "force_combo is false and only one aggregate provided, but avg_median is specified");
            }
            if (aggregate.getType() != ComboAggregateRequest.AggType.COUNT
                    && aggregate.getType() != ComboAggregateRequest.AggType.WEIGHTED_AVG) {
                if (!aggregate.getColumnName().isEmpty()) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "force_combo is false and only one aggregate provided, but column_name is specified for type other than COUNT or WEIGHTED_AVG");
                }
            }
        } else {
            for (ComboAggregateRequest.Aggregate aggregate : request.getAggregatesList()) {
                if (aggregate.getType() != ComboAggregateRequest.AggType.PERCENTILE) {
                    if (aggregate.getPercentile() != 0) {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "percentile is specified for type " + aggregate.getType());
                    }
                    if (aggregate.getAvgMedian()) {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "avg_median is specified for type " + aggregate.getType());
                    }
                }
                if (aggregate.getType() == ComboAggregateRequest.AggType.COUNT) {
                    if (aggregate.getMatchPairsCount() != 0) {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "match_pairs is specified for type COUNT");
                    }
                }
                if (aggregate.getType() != ComboAggregateRequest.AggType.COUNT
                        && aggregate.getType() != ComboAggregateRequest.AggType.WEIGHTED_AVG) {
                    if (!aggregate.getColumnName().isEmpty()) {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "column_name is specified for type " + aggregate.getType());
                    }
                }
            }
        }
    }

    private boolean isSimpleAggregation(ComboAggregateRequest request) {
        return !request.getForceCombo() && request.getAggregatesCount() == 1
                && request.getAggregates(0).getColumnName().isEmpty()
                && request.getAggregates(0).getType() != ComboAggregateRequest.AggType.PERCENTILE
                && request.getAggregates(0).getMatchPairsCount() == 0;
    }

    @Override
    public Table create(final ComboAggregateRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table parent = sourceTables.get(0).get();
        final String[] groupBySpecs = request.getGroupByColumnsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final SelectColumn[] groupByColumns = SelectColumnFactory.getExpressions(groupBySpecs);
        ColumnExpressionValidator.validateColumnExpressions(groupByColumns, groupBySpecs, parent);

        final Table result;
        if (isSimpleAggregation(request)) {
            // This is a special case with a special operator that can be invoked right off of the table api.
            result = singleAggregateHelper(parent, groupByColumns, request.getAggregates(0));
        } else {
            result = comboAggregateHelper(parent, groupByColumns, request.getAggregatesList());
        }
        return result;
    }

    private static Table singleAggregateHelper(final Table parent, final SelectColumn[] groupByColumns,
            final ComboAggregateRequest.Aggregate aggregate) {
        switch (aggregate.getType()) {
            case SUM:
                return parent.sumBy(groupByColumns);
            case ABS_SUM:
                return parent.absSumBy(groupByColumns);
            case GROUP:
                return parent.groupBy(Arrays.asList(groupByColumns));
            case AVG:
                return parent.avgBy(groupByColumns);
            case COUNT:
                return parent.countBy(aggregate.getColumnName(), groupByColumns);
            case FIRST:
                return parent.firstBy(groupByColumns);
            case LAST:
                return parent.lastBy(groupByColumns);
            case MIN:
                return parent.minBy(groupByColumns);
            case MAX:
                return parent.maxBy(groupByColumns);
            case MEDIAN:
                return parent.medianBy(groupByColumns);
            case STD:
                return parent.stdBy(groupByColumns);
            case VAR:
                return parent.varBy(groupByColumns);
            case WEIGHTED_AVG:
                return parent.wavgBy(aggregate.getColumnName(), groupByColumns);
            default:
                throw new UnsupportedOperationException("Unsupported aggregate: " + aggregate.getType());
        }
    }

    private static Table comboAggregateHelper(final Table parent, final SelectColumn[] groupByColumns,
            final List<ComboAggregateRequest.Aggregate> aggregates) {
        final Set<String> groupByColumnSet =
                Arrays.stream(groupByColumns).map(SelectColumn::getName).collect(Collectors.toSet());
        final Function<ComboAggregateRequest.Aggregate, String[]> getPairs =
                agg -> getColumnPairs(parent, groupByColumnSet, agg);

        final Collection<? extends Aggregation> aggregations = aggregates.stream().map(
                agg -> makeAggregation(agg, getPairs)).collect(Collectors.toList());

        return parent.aggBy(aggregations, Arrays.asList(groupByColumns));
    }

    private static String[] getColumnPairs(@NotNull final Table parent,
            @NotNull final Set<String> groupByColumnSet,
            @NotNull final ComboAggregateRequest.Aggregate agg) {
        if (agg.getMatchPairsCount() == 0) {
            // If not specified, we apply the aggregate to all columns not "otherwise involved"
            return parent.getDefinition().getColumnStream()
                    .map(ColumnDefinition::getName)
                    .filter(n -> !(groupByColumnSet.contains(n) ||
                            (agg.getType() == ComboAggregateRequest.AggType.WEIGHTED_AVG
                                    && agg.getColumnName().equals(n))))
                    .toArray(String[]::new);
        }
        return agg.getMatchPairsList().toArray(String[]::new);
    }

    private static Aggregation makeAggregation(
            @NotNull final ComboAggregateRequest.Aggregate agg,
            @NotNull final Function<ComboAggregateRequest.Aggregate, String[]> getPairs) {
        switch (agg.getType()) {
            case SUM:
                return AggSum(getPairs.apply(agg));
            case ABS_SUM:
                return AggAbsSum(getPairs.apply(agg));
            case GROUP:
                return AggGroup(getPairs.apply(agg));
            case AVG:
                return Aggregation.AggAvg(getPairs.apply(agg));
            case COUNT:
                return Aggregation.AggCount(agg.getColumnName());
            case FIRST:
                return Aggregation.AggFirst(getPairs.apply(agg));
            case LAST:
                return Aggregation.AggLast(getPairs.apply(agg));
            case MIN:
                return Aggregation.AggMin(getPairs.apply(agg));
            case MAX:
                return Aggregation.AggMax(getPairs.apply(agg));
            case MEDIAN:
                return Aggregation.AggMed(getPairs.apply(agg));
            case PERCENTILE:
                return Aggregation.AggPct(agg.getPercentile(), agg.getAvgMedian(), getPairs.apply(agg));
            case STD:
                return Aggregation.AggStd(getPairs.apply(agg));
            case VAR:
                return Aggregation.AggVar(getPairs.apply(agg));
            case WEIGHTED_AVG:
                return Aggregation.AggWAvg(agg.getColumnName(), getPairs.apply(agg));
            default:
                throw new UnsupportedOperationException("Unsupported aggregate: " + agg.getType());
        }
    }
}
