package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest;
import io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest.Builder;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class SnapshotWhenGrpcTestBase extends GrpcTableOperationTestBase<SnapshotWhenTableRequest> {

    public abstract Builder builder();

    @Override
    public ExportedTableCreationResponse send(SnapshotWhenTableRequest snapshotTableRequest) {
        return channel().tableBlocking().snapshotWhen(snapshotTableRequest);
    }

    @Test
    public void snapshotTicking() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01").view("Id=ii"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotTickingDoInitial() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01").view("Id=ii"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .setInitial(true)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotTickingStamp() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01").view("Id=ii"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02").updateView("Id=ii"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .addStampColumns("Timestamp")
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotTickingStampRename() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .addStampColumns("StampTimestamp=Timestamp")
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotTickingStampDoInitial() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01").view("Id=ii"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02").updateView("Id=ii"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .addStampColumns("Timestamp")
                .setInitial(true)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotTickingNoStamps() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02").dropColumns("Timestamp"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void snapshotTickingNoStampsDoInitial() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02").dropColumns("Timestamp"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .setInitial(true)
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        } finally {
            release(response);
        }
    }

    @Test
    public void missingResultId() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01").view("Id=ii"));
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02"));
        final SnapshotWhenTableRequest request = builder()
                .setBaseId(timeTable1)
                .setTriggerId(timeTable2)
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingBaseId() {
        final TableReference timeTable2 = ref(TableTools.timeTable("00:00:02"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setTriggerId(timeTable2)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest must have field base_id (2)");
    }

    @Test
    public void missingTriggerId() {
        final TableReference timeTable1 = ref(TableTools.timeTable("00:00:01").view("Id=ii"));
        final SnapshotWhenTableRequest request = builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setBaseId(timeTable1)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.SnapshotWhenTableRequest must have field trigger_id (3)");
    }
}
