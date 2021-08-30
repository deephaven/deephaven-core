package io.deephaven.grpc_api.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.table.validation.ColumnExpressionValidator;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ComboAggregateRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
            case ARRAY:
                return parent.by(groupByColumns);
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

        final ComboAggregateFactory.ComboBy[] comboBy =
                new ComboAggregateFactory.ComboBy[aggregates.size()];

        for (int i = 0; i < aggregates.size(); i++) {
            final ComboAggregateRequest.Aggregate agg = aggregates.get(i);

            final String[] matchPairs;
            if (agg.getMatchPairsCount() == 0) {
                // if not specified, we apply the aggregate to all columns not "otherwise involved"
                matchPairs = Arrays.stream(parent.getColumns())
                        .map(DataColumn::getName)
                        .filter(n -> !(groupByColumnSet.contains(n)
                                || (agg.getType() == ComboAggregateRequest.AggType.WEIGHTED_AVG
                                        && agg.getColumnName().equals(n))))
                        .toArray(String[]::new);
            } else {
                matchPairs = agg.getMatchPairsList().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                final SelectColumn[] matchPairExpressions = SelectColumnFactory.getExpressions(matchPairs);
                ColumnExpressionValidator.validateColumnExpressions(matchPairExpressions, matchPairs, parent);
            }

            final Supplier<ComboAggregateFactory.ComboBy> comboMapper = () -> {
                switch (agg.getType()) {
                    case SUM:
                        return ComboAggregateFactory.AggSum(matchPairs);
                    case ABS_SUM:
                        return ComboAggregateFactory.AggAbsSum(matchPairs);
                    case ARRAY:
                        return ComboAggregateFactory.AggArray(matchPairs);
                    case AVG:
                        return ComboAggregateFactory.AggAvg(matchPairs);
                    case COUNT:
                        return ComboAggregateFactory.AggCount(agg.getColumnName());
                    case FIRST:
                        return ComboAggregateFactory.AggFirst(matchPairs);
                    case LAST:
                        return ComboAggregateFactory.AggLast(matchPairs);
                    case MIN:
                        return ComboAggregateFactory.AggMin(matchPairs);
                    case MAX:
                        return ComboAggregateFactory.AggMax(matchPairs);
                    case MEDIAN:
                        return ComboAggregateFactory.AggMed(matchPairs);
                    case PERCENTILE:
                        return ComboAggregateFactory.AggPct(agg.getPercentile(), agg.getAvgMedian(), matchPairs);
                    case STD:
                        return ComboAggregateFactory.AggStd(matchPairs);
                    case VAR:
                        return ComboAggregateFactory.AggVar(matchPairs);
                    case WEIGHTED_AVG:
                        return ComboAggregateFactory.AggWAvg(agg.getColumnName(), matchPairs);
                    default:
                        throw new UnsupportedOperationException("Unsupported aggregate: " + agg.getType());
                }
            };

            comboBy[i] = comboMapper.get();
        }

        return parent.by(ComboAggregateFactory.AggCombo(comboBy), groupByColumns);
    }
}
