//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.BadDataBehavior;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.UpdateByEmOptions;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEmStd;
import io.deephaven.proto.backplane.grpc.UpdateByWindowScale;
import io.deephaven.proto.backplane.grpc.UpdateByWindowScale.UpdateByWindowTicks;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the on_null_value / on_nan_value option handling in adaptEmOptions. The original code had two bugs: the
 * null-value guard read getOnNanValue() instead of getOnNullValue(), and both guards used == (apply when NOT_SPECIFIED)
 * instead of != (apply when specified).
 */
public class UpdateByEmOptionsGrpcTest extends GrpcTableOperationTestBase<UpdateByRequest> {

    @Override
    public ExportedTableCreationResponse send(UpdateByRequest request) {
        return channel().tableBlocking().updateBy(request);
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private static UpdateByWindowScale ticksScale(double ticks) {
        return UpdateByWindowScale.newBuilder()
                .setTicks(UpdateByWindowTicks.newBuilder().setTicks(ticks).build())
                .build();
    }

    private static UpdateByOperation emaOperation(UpdateByEmOptions options, String column) {
        UpdateByEma.Builder ema = UpdateByEma.newBuilder().setWindowScale(ticksScale(5));
        if (options != null) {
            ema.setOptions(options);
        }
        return UpdateByOperation.newBuilder()
                .setColumn(UpdateByColumn.newBuilder()
                        .setSpec(UpdateBySpec.newBuilder().setEma(ema).build())
                        .addMatchPairs(column + "=Value")
                        .build())
                .build();
    }

    private static UpdateByOperation emStdOperation(UpdateByEmOptions options, String column) {
        UpdateByEmStd.Builder emStd = UpdateByEmStd.newBuilder().setWindowScale(ticksScale(5));
        if (options != null) {
            emStd.setOptions(options);
        }
        return UpdateByOperation.newBuilder()
                .setColumn(UpdateByColumn.newBuilder()
                        .setSpec(UpdateBySpec.newBuilder().setEmStd(emStd).build())
                        .addMatchPairs(column + "=Value")
                        .build())
                .build();
    }

    private UpdateByRequest buildRequest(int exportId, TableReference sourceRef, UpdateByOperation op) {
        return UpdateByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(exportId))
                .setSourceId(sourceRef)
                .addOperations(op)
                .build();
    }

    // -----------------------------------------------------------------------
    // Bug A: null guard read getOnNanValue() instead of getOnNullValue()
    // -----------------------------------------------------------------------

    /**
     * on_null_value set, on_nan_value absent (NOT_SPECIFIED). Old null guard: getOnNanValue()==NOT_SPECIFIED → true →
     * nan branch also fires → adaptBadDataBehavior(NOT_SPECIFIED) → IllegalArgumentException.
     */
    @Test
    public void emaWithOnlyOnNullValue() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final UpdateByEmOptions options = UpdateByEmOptions.newBuilder()
                .setOnNullValue(BadDataBehavior.SKIP)
                .build();
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emaOperation(options, "Ema")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    @Test
    public void emStdWithOnlyOnNullValue() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final UpdateByEmOptions options = UpdateByEmOptions.newBuilder()
                .setOnNullValue(BadDataBehavior.SKIP)
                .build();
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emStdOperation(options, "EmStd")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    /**
     * Empty UpdateByEmOptions message (hasOptions=true, all fields NOT_SPECIFIED). Old guards both fired →
     * adaptBadDataBehavior(NOT_SPECIFIED) → IllegalArgumentException.
     */
    @Test
    public void emaWithEmptyOptions() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emaOperation(UpdateByEmOptions.getDefaultInstance(), "Ema")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    @Test
    public void emStdWithEmptyOptions() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emStdOperation(UpdateByEmOptions.getDefaultInstance(), "EmStd")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    /**
     * Regression for Bug A when both options are set: old null guard used getOnNanValue(), so with onNanValue=SKIP
     * (non-NOT_SPECIFIED) the guard was false and onNullValue=THROW was silently dropped. The operation then succeeded
     * on null input instead of throwing.
     */
    @Test
    public void emaThrowsOnNullValueWhenBothOptionsSetAndNullInput() {
        final TableReference ref = ref(TableTools.emptyTable(3)
                .update("Value = (double) io.deephaven.util.QueryConstants.NULL_DOUBLE"));
        assertThatThrownBy(() -> send(buildRequest(1, ref,
                emaOperation(UpdateByEmOptions.newBuilder()
                        .setOnNullValue(BadDataBehavior.THROW)
                        .setOnNanValue(BadDataBehavior.SKIP)
                        .build(), "Ema"))))
                .isInstanceOf(StatusRuntimeException.class);
    }

    // -----------------------------------------------------------------------
    // Bug B: nan guard used == instead of !=, silently dropping explicit values
    // -----------------------------------------------------------------------

    /**
     * Regression for Bug B: old nan guard getOnNanValue()==NOT_SPECIFIED was false when onNanValue=THROW, so THROW was
     * silently dropped and the default SKIP applied. The operation then succeeded on NaN input instead of throwing.
     */
    @Test
    public void emaThrowsOnNanValueWhenOnNanValueIsThrow() {
        final TableReference ref =
                ref(TableTools.emptyTable(3).update("Value = Double.NaN"));
        assertThatThrownBy(() -> send(buildRequest(1, ref,
                emaOperation(UpdateByEmOptions.newBuilder()
                        .setOnNanValue(BadDataBehavior.THROW)
                        .build(), "Ema"))))
                .isInstanceOf(StatusRuntimeException.class);
    }
}
