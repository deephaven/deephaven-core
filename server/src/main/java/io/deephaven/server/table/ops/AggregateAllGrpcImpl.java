//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.proto.backplane.grpc.AggSpec.TypeCase;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public final class AggregateAllGrpcImpl extends GrpcTableOperation<AggregateAllRequest> {

    private final ColumnExpressionValidator expressionValidator;

    @Inject
    public AggregateAllGrpcImpl(final TableServiceContextualAuthWiring authWiring,
            final ColumnExpressionValidator expressionValidator) {
        super(
                authWiring::checkPermissionAggregateAll,
                BatchTableRequest.Operation::getAggregateAll,
                AggregateAllRequest::getResultId,
                AggregateAllRequest::getSourceId);
        this.expressionValidator = expressionValidator;
    }

    @Override
    public void validateRequest(AggregateAllRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, AggregateAllRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasField(request, AggregateAllRequest.SPEC_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getSourceId());
        AggSpecAdapter.validate(request.getSpec());
    }

    @Override
    public Table create(AggregateAllRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        Assert.eqTrue(request.hasSpec(), "request.hasSpec()");
        Assert.neq(request.getSpec().getTypeCase(), "request.getSpec().getTypeCase()", TypeCase.TYPE_NOT_SET);
        final Table parent = sourceTables.get(0).get();

        if (request.getSpec().getTypeCase() == TypeCase.FORMULA) {
            final List<ColumnName> groupByColumns = ColumnName.from(request.getGroupByColumnsList());
            AggSpecFormulaValidator.validate(
                    request.getSpec().getFormula(),
                    parent,
                    groupByColumns,
                    AggSpecFormulaValidator.nonKeyColumnNames(parent, groupByColumns),
                    expressionValidator);
        }

        final AggSpec spec = AggSpecAdapter.adapt(request.getSpec());
        return parent.aggAllBy(spec, request.getGroupByColumnsList());
    }
}
