//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest.Builder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SnapshotWhenHistoryGrpcTest extends SnapshotWhenGrpcTestBase {

    private static void checkUnimplemented(StatusRuntimeException e) {
        assertThat(e.getStatus().getCode()).isEqualTo(Code.UNIMPLEMENTED);
        assertThat(e).hasMessageContaining("snapshotWhen with history does not currently support");
    }

    @Override
    public Builder builder() {
        return SnapshotWhenTableRequest.newBuilder().setHistory(true);
    }

    @Override
    public void snapshotTickingDoInitial() {
        try {
            super.snapshotTickingDoInitial();
        } catch (StatusRuntimeException e) {
            checkUnimplemented(e);
            return;
        }
        failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
    }

    @Override
    public void snapshotTickingStampDoInitial() {
        try {
            super.snapshotTickingStampDoInitial();
        } catch (StatusRuntimeException e) {
            checkUnimplemented(e);
            return;
        }
        failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
    }

    @Override
    public void snapshotTickingNoStampsDoInitial() {
        try {
            super.snapshotTickingNoStampsDoInitial();
        } catch (StatusRuntimeException e) {
            checkUnimplemented(e);
            return;
        }
        failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
    }
}
