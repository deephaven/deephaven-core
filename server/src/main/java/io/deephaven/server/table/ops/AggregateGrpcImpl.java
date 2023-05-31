package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public final class AggregateGrpcImpl extends GrpcTableOperation<AggregateRequest> {
    private static List<TableReference> refs(AggregateRequest request) {
        return request.hasInitialGroupsId() ? List.of(request.getSourceId(), request.getInitialGroupsId())
                : List.of(request.getSourceId());
    }

    @Inject
    public AggregateGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(
                authWiring::checkPermissionAggregate,
                BatchTableRequest.Operation::getAggregate,
                AggregateRequest::getResultId,
                AggregateGrpcImpl::refs);
    }

    @Override
    public void validateRequest(AggregateRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, AggregateRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkRepeatedFieldNonEmpty(request, AggregateRequest.AGGREGATIONS_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getSourceId());
        if (request.hasInitialGroupsId()) {
            Common.validate(request.getInitialGroupsId());
        }
        for (Aggregation aggregation : request.getAggregationsList()) {
            AggregationAdapter.validate(aggregation);
        }
    }

    @Override
    public Table create(AggregateRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", request.hasInitialGroupsId() ? 2 : 1);
        Assert.gtZero(request.getAggregationsCount(), "request.getAggregationsCount()");
        final Table parent = sourceTables.get(0).get();
        final Table initialGroups = request.hasInitialGroupsId() ? sourceTables.get(1).get() : null;
        final List<io.deephaven.api.agg.Aggregation> aggregations = request.getAggregationsList()
                .stream()
                .map(AggregationAdapter::adapt)
                .collect(Collectors.toList());
        final List<ColumnName> groupByColumns = ColumnName.from(request.getGroupByColumnsList());
        return parent.aggBy(aggregations, request.getPreserveEmpty(), initialGroups, groupByColumns);
    }
}
