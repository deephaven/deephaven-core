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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the four on_null_value / on_nan_value option combinations in adaptEmOptions. The original code had two
 * bugs: the null-value guard read getOnNanValue() instead of getOnNullValue(), and both guards used == (apply when
 * NOT_SPECIFIED) instead of != (apply when specified).
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
    // tests — four option combinations, exercised via EMA and EmStd
    // -----------------------------------------------------------------------

    /**
     * Neither on_null_value nor on_nan_value set (the common default). The server should use its own defaults and
     * succeed. Bug (A) caused adaptBadDataBehavior(NOT_SPECIFIED) to be called → IllegalArgumentException.
     */
    @Test
    public void emaWithNoOptions() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emaOperation(null, "Ema")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    /**
     * Only on_null_value set. The server should apply it and succeed. Before the fix: bug (A) meant the wrong field was
     * guarded, bug (B) threw on NOT_SPECIFIED for nan.
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

    /**
     * Only on_nan_value set. The server should apply it and succeed. Bug (B) previously dropped it silently when the
     * guard was `==`.
     */
    @Test
    public void emaWithOnlyOnNanValue() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final UpdateByEmOptions options = UpdateByEmOptions.newBuilder()
                .setOnNanValue(BadDataBehavior.RESET)
                .build();
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emaOperation(options, "Ema")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    /**
     * Both on_null_value and on_nan_value set. The server should apply both and succeed. Bug caused both guards to be
     * false → both options silently dropped.
     */
    @Test
    public void emaWithBothOptions() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final UpdateByEmOptions options = UpdateByEmOptions.newBuilder()
                .setOnNullValue(BadDataBehavior.SKIP)
                .setOnNanValue(BadDataBehavior.RESET)
                .build();
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emaOperation(options, "Ema")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    /**
     * Same four cases via EmStd to verify the fix applies to all EM aggregations.
     */
    @Test
    public void emStdWithNoOptions() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emStdOperation(null, "EmStd")));
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

    @Test
    public void emStdWithOnlyOnNanValue() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final UpdateByEmOptions options = UpdateByEmOptions.newBuilder()
                .setOnNanValue(BadDataBehavior.RESET)
                .build();
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emStdOperation(options, "EmStd")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }

    @Test
    public void emStdWithBothOptions() {
        final TableReference ref =
                ref(TableTools.emptyTable(10).update("Value = (double) ii"));
        final UpdateByEmOptions options = UpdateByEmOptions.newBuilder()
                .setOnNullValue(BadDataBehavior.SKIP)
                .setOnNanValue(BadDataBehavior.RESET)
                .build();
        final ExportedTableCreationResponse response =
                send(buildRequest(1, ref, emStdOperation(options, "EmStd")));
        assertThat(response.getSuccess()).isTrue();
        release(response);
    }
}
