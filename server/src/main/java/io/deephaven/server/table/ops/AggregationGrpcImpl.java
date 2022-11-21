package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.AggregationRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.api.TableOperations.AGG_BY_PRESERVE_EMPTY_DEFAULT;

@Singleton
public final class AggregationGrpcImpl extends GrpcTableOperation<AggregationRequest> {
    private static List<TableReference> refs(AggregationRequest request) {
        return request.hasInitialGroupsId() ? List.of(request.getSourceId(), request.getInitialGroupsId())
                : List.of(request.getSourceId());
    }

    @Inject
    public AggregationGrpcImpl() {
        super(BatchTableRequest.Operation::getAggregation, AggregationRequest::getResultId, AggregationGrpcImpl::refs);
    }

    @Override
    public void validateRequest(AggregationRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, AggregationRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkRepeatedFieldNonEmpty(request, AggregationRequest.AGGREGATIONS_FIELD_NUMBER);
        for (Aggregation aggregation : request.getAggregationsList()) {
            AggregationAdapter.validate(aggregation);
        }
    }

    @Override
    public Table create(AggregationRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", request.hasInitialGroupsId() ? 2 : 1);
        Assert.gtZero(request.getAggregationsCount(), "request.getAggregationsCount()");
        final Table parent = sourceTables.get(0).get();
        final Table initialGroups = request.hasInitialGroupsId() ? sourceTables.get(1).get() : null;
        final List<io.deephaven.api.agg.Aggregation> aggregations = request.getAggregationsList()
                .stream()
                .map(AggregationAdapter::adapt)
                .collect(Collectors.toList());
        final List<ColumnName> groupByColumns = ColumnName.from(request.getGroupByColumnsList());
        final boolean preserveEmpty =
                request.hasPreserveEmpty() ? request.getPreserveEmpty() : AGG_BY_PRESERVE_EMPTY_DEFAULT;
        return parent.aggBy(aggregations, preserveEmpty, initialGroups, groupByColumns);
    }
}
