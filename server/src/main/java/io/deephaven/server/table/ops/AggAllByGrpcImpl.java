package io.deephaven.server.table.ops;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.AggAllByRequest;
import io.deephaven.proto.backplane.grpc.AggSpec.TypeCase;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public final class AggAllByGrpcImpl extends GrpcTableOperation<AggAllByRequest> {

    @Inject
    public AggAllByGrpcImpl() {
        super(BatchTableRequest.Operation::getAggAllBy, AggAllByRequest::getResultId, AggAllByRequest::getSourceId);
    }

    @Override
    public void validateRequest(AggAllByRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, AggAllByRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AggAllByRequest.SPEC_FIELD_NUMBER);
        AggSpecAdapter.validate(request.getSpec());
    }

    @Override
    public Table create(AggAllByRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        Assert.eqTrue(request.hasSpec(), "request.hasSpec()");
        Assert.neq(request.getSpec().getTypeCase(), "request.getSpec().getTypeCase()", TypeCase.TYPE_NOT_SET);
        final Table parent = sourceTables.get(0).get();
        final AggSpec spec = AggSpecAdapter.adapt(request.getSpec());
        return parent.aggAllBy(spec, request.getGroupByColumnsList());
    }
}
