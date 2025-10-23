//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.api.ColumnName;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.backplane.grpc.Aggregation;
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
    private final ColumnExpressionValidator expressionValidator;

    private static List<TableReference> refs(AggregateRequest request) {
        return request.hasInitialGroupsId() ? List.of(request.getSourceId(), request.getInitialGroupsId())
                : List.of(request.getSourceId());
    }

    @Inject
    public AggregateGrpcImpl(final TableServiceContextualAuthWiring authWiring,
            final ColumnExpressionValidator expressionValidator) {
        super(
                authWiring::checkPermissionAggregate,
                BatchTableRequest.Operation::getAggregate,
                AggregateRequest::getResultId,
                AggregateGrpcImpl::refs);
        this.expressionValidator = expressionValidator;
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
        final List<ColumnName> groupByColumns = ColumnName.from(request.getGroupByColumnsList());

        request.getAggregationsList().forEach(agg -> validateFormulas(agg, parent, groupByColumns));
        final List<io.deephaven.api.agg.Aggregation> aggregations = request.getAggregationsList()
                .stream()
                .map(AggregationAdapter::adapt)
                .collect(Collectors.toList());
        return parent.aggBy(aggregations, request.getPreserveEmpty(), initialGroups, groupByColumns);
    }

    private void validateFormulas(Aggregation agg, Table parent, List<ColumnName> groupByColumns) {
        if (agg.hasCountWhere()) {
            final String[] filters = agg.getCountWhere().getFiltersList().toArray(String[]::new);
            expressionValidator.validateSelectFilters(filters, parent.getDefinition());
        }
        if (agg.hasFormula()) {
            final Selectable selectableGrpc = agg.getFormula().getSelectable();
            switch (selectableGrpc.getTypeCase()) {
                case RAW:
                    final String selectableRaw = selectableGrpc.getRaw();
                    io.deephaven.api.Selectable selectableParsed = io.deephaven.api.Selectable.parse(selectableRaw);
                    final SelectColumn sc = SelectColumn.of(selectableParsed);

                    // We need to create the definition that would be used by the formula, which is just the parent
                    // table, but aggregated. We create an empty table with the appropriate definition.
                    final Table parentPrototype = TableTools.newTable(parent.getDefinition());
                    final Table formulaPrototype = parentPrototype.groupBy(groupByColumns);
                    expressionValidator.validateColumnExpressions(new SelectColumn[] {sc}, new String[] {selectableRaw},
                            formulaPrototype.getDefinition());
                case TYPE_NOT_SET:
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported Selectable type (" + selectableGrpc.getTypeCase() + ") in Aggregation.");
            }
        }
    }
}
