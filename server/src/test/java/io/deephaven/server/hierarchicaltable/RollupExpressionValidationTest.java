//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.hierarchicaltable;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCountWhere;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationFormula;
import io.deephaven.proto.backplane.grpc.HierarchicalTableServiceGrpc;
import io.deephaven.proto.backplane.grpc.RollupRequest;
import io.deephaven.proto.backplane.grpc.RollupResponse;
import io.deephaven.proto.backplane.grpc.Selectable;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.table.ops.GrpcTableOperationTestBase;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Regression test: {@code HierarchicalTableService#rollup} must run user-supplied aggregation expressions through the
 * {@link io.deephaven.engine.validation.ColumnExpressionValidator}, consistent with {@code TableService#aggregate}
 * ({@code AggregateGrpcImpl.validateFormulas}) which validates the identical {@code Aggregation} messages.
 * <p>
 * Before the fix, the rollup path skipped the validator entirely, so an {@code AggCountWhere} filter containing a
 * disallowed expression compiled and executed. The chosen expression ({@code Runtime.getRuntime() == null}) is rejected
 * by the parsed validator but side-effect-free at runtime.
 */
public class RollupExpressionValidationTest extends GrpcTableOperationTestBase<RollupRequest> {

    @Override
    public io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse send(RollupRequest request) {
        // Not used; rollup returns a RollupResponse rather than an ExportedTableCreationResponse.
        throw new UnsupportedOperationException();
    }

    private RollupResponse rollup(RollupRequest request) {
        final HierarchicalTableServiceGrpc.HierarchicalTableServiceBlockingStub stub =
                HierarchicalTableServiceGrpc.newBlockingStub(channel().channel());
        return stub.rollup(request);
    }

    private Ticket sourceTicket(Table table) {
        final ExportObject<Table> export = authenticatedSessionState().newServerSideExport(table);
        return export.getExportId();
    }

    @Test
    public void rollupCountWhereRejectsDisallowedExpression() {
        final Ticket source = sourceTicket(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final RollupRequest request = RollupRequest.newBuilder()
                .setResultRollupTableId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceTableId(source)
                .addAggregations(Aggregation.newBuilder()
                        .setCountWhere(AggregationCountWhere.newBuilder()
                                .setColumnName("Cnt")
                                // Disallowed by the parsed ColumnExpressionValidator (method invocation on Runtime),
                                // but harmless if it were to execute.
                                .addFilters("Runtime.getRuntime() == null")
                                .build())
                        .build())
                .addGroupByColumns("Key")
                .build();

        // The disallowed expression must be rejected before compilation, mirroring
        // AggregateGrpcTest#columnsWithFormulaRejectsDisallowedExpression.
        try {
            final RollupResponse response = rollup(request);
            release(ExportTicketHelper.wrapExportIdInTicket(1));
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
        }
    }

    @Test
    public void rollupFormulaRejectsDisallowedExpression() {
        final Ticket source = sourceTicket(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final RollupRequest request = RollupRequest.newBuilder()
                .setResultRollupTableId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceTableId(source)
                .addAggregations(Aggregation.newBuilder()
                        .setFormula(AggregationFormula.newBuilder()
                                .setSelectable(Selectable.newBuilder()
                                        // Disallowed method invocation on Runtime, side-effect-free if it ran.
                                        .setRaw("Pwned = Runtime.getRuntime().hashCode()")
                                        .build())
                                .build())
                        .build())
                .addGroupByColumns("Key")
                .build();

        try {
            final RollupResponse response = rollup(request);
            release(ExportTicketHelper.wrapExportIdInTicket(1));
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
        }
    }

    @Test
    public void rollupCountWhereAcceptsBenignExpression() {
        final Ticket source = sourceTicket(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final RollupRequest request = RollupRequest.newBuilder()
                .setResultRollupTableId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceTableId(source)
                .addAggregations(Aggregation.newBuilder()
                        .setCountWhere(AggregationCountWhere.newBuilder()
                                .setColumnName("Cnt")
                                .addFilters("I > 5")
                                .build())
                        .build())
                .addGroupByColumns("Key")
                .build();

        final RollupResponse response = rollup(request);
        assertThat(response).isNotNull();
        release(ExportTicketHelper.wrapExportIdInTicket(1));
    }

    /**
     * A formula that references the engine's synthetic rollup columns ({@code __FORMULA_DEPTH__} /
     * {@code __FORMULA_KEYS__}) must validate and build successfully. This guards the validation prototype built by
     * {@code makeRollupFormulaPrototype}: if it omitted those columns, or gave them the wrong type, the validator would
     * reject a legitimate request even though the engine compiles it (the engine adds them via
     * {@code EXTRA_ROLLUP_FORMULA_DEFINITIONS}). The formulas mirror {@code TestRollupTable}.
     */
    @Test
    public void rollupFormulaReferencingSyntheticColumnsSucceeds() {
        final Ticket source = sourceTicket(TableTools.emptyTable(100).view("Key=ii % 2", "Sentinel=(int)ii"));
        final RollupRequest request = RollupRequest.newBuilder()
                .setResultRollupTableId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceTableId(source)
                .addAggregations(Aggregation.newBuilder()
                        .setFormula(AggregationFormula.newBuilder()
                                .setSelectable(Selectable.newBuilder()
                                        .setRaw("FSum = __FORMULA_DEPTH__ == 0 ? max(Sentinel) : 1 + sum(Sentinel)")
                                        .build())
                                .build())
                        .build())
                .addAggregations(Aggregation.newBuilder()
                        .setFormula(AggregationFormula.newBuilder()
                                .setSelectable(Selectable.newBuilder()
                                        .setRaw("KeyColumns = __FORMULA_KEYS__")
                                        .build())
                                .build())
                        .build())
                .addGroupByColumns("Key")
                .build();

        final RollupResponse response = rollup(request);
        assertThat(response).isNotNull();
        release(ExportTicketHelper.wrapExportIdInTicket(1));
    }
}
