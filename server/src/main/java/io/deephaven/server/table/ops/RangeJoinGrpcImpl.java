package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RangeEndRule;
import io.deephaven.api.RangeJoinMatch;
import io.deephaven.api.RangeStartRule;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public final class RangeJoinGrpcImpl extends GrpcTableOperation<RangeJoinTablesRequest> {

    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public RangeJoinGrpcImpl(
            final TableServiceContextualAuthWiring authWiring,
            final UpdateGraphProcessor updateGraphProcessor) {
        super(
                authWiring::checkPermissionRangeJoinTables,
                BatchTableRequest.Operation::getRangeJoin,
                RangeJoinTablesRequest::getResultId,
                RangeJoinGrpcImpl::refs);
        this.updateGraphProcessor = updateGraphProcessor;
    }

    private static List<TableReference> refs(RangeJoinTablesRequest request) {
        return List.of(request.getLeftId(), request.getRightId());
    }

    @Override
    public void validateRequest(RangeJoinTablesRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.LEFT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.RIGHT_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.LEFT_START_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.RANGE_START_RULE_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.RIGHT_RANGE_COLUMN_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.RANGE_END_RULE_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, RangeJoinTablesRequest.LEFT_END_COLUMN_FIELD_NUMBER);

        GrpcErrorHelper.checkRepeatedFieldNonEmpty(request, RangeJoinTablesRequest.AGGREGATIONS_FIELD_NUMBER);

        GrpcErrorHelper.checkHasNoUnknownFields(request);

        Common.validate(request.getLeftId());
        Common.validate(request.getRightId());

        for (String exactMatch : request.getExactMatchColumnsList()) {
            JoinMatch.parse(exactMatch);
        }

        parseRangeMatch(request);

        for (Aggregation aggregation : request.getAggregationsList()) {
            AggregationAdapter.validate(aggregation);
        }
    }

    @Override
    public Table create(RangeJoinTablesRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 2);
        Assert.gtZero(request.getAggregationsCount(), "request.getAggregationsCount()");

        final Table leftTable = sourceTables.get(0).get();
        final Table rightTable = sourceTables.get(1).get();
        final Collection<JoinMatch> exactMatches = JoinMatch.from(request.getExactMatchColumnsList());
        final RangeJoinMatch rangeMatch = parseRangeMatch(request);
        final Collection<? extends io.deephaven.api.agg.Aggregation> aggregations = request.getAggregationsList()
                .stream()
                .map(AggregationAdapter::adapt)
                .collect(Collectors.toList());

        if (!leftTable.isRefreshing() && !rightTable.isRefreshing()) {
            return leftTable.rangeJoin(rightTable, exactMatches, rangeMatch, aggregations);
        } else {
            return updateGraphProcessor.sharedLock().computeLocked(
                    () -> leftTable.rangeJoin(rightTable, exactMatches, rangeMatch, aggregations));
        }
    }

    private static RangeJoinMatch parseRangeMatch(@NotNull final RangeJoinTablesRequest request) {
        final RangeStartRule rangeStartRule;
        switch (request.getRangeStartRule()) {
            case LESS_THAN:
                rangeStartRule = RangeStartRule.LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                rangeStartRule = RangeStartRule.LESS_THAN_OR_EQUAL;
                break;
            case LESS_THAN_OR_EQUAL_ALLOW_PRECEDING:
                rangeStartRule = RangeStartRule.LESS_THAN_OR_EQUAL_ALLOW_PRECEDING;
                break;
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        String.format("Unrecognized range start rule %s for range join",
                                request.getRangeStartRule()));
        }

        final RangeEndRule rangeEndRule;
        switch (request.getRangeEndRule()) {
            case GREATER_THAN:
                rangeEndRule = RangeEndRule.GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                rangeEndRule = RangeEndRule.GREATER_THAN_OR_EQUAL;
                break;
            case GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING:
                rangeEndRule = RangeEndRule.GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING;
                break;
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        String.format("Unrecognized range end rule %s for range join",
                                request.getRangeEndRule()));
        }

        return RangeJoinMatch.of(
                ColumnName.parse(request.getLeftStartColumn()),
                rangeStartRule,
                ColumnName.parse(request.getRightRangeColumn()),
                rangeEndRule,
                ColumnName.parse(request.getLeftEndColumn()));
    }
}
