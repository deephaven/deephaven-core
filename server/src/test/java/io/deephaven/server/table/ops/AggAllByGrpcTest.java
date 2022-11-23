package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.AggAllByRequest;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecBlank;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AggAllByGrpcTest extends GrpcTableOperationTestBase<AggAllByRequest> {

    @Override
    public ExportedTableCreationResponse send(AggAllByRequest aggAllByRequest) {
        return channel().tableBlocking().aggAllBy(aggAllByRequest);
    }

    @Test
    public void aggAllBy() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggAllByRequest request = AggAllByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecBlank.getDefaultInstance()).build())
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggAllBy(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }

    @Test
    public void aggAllByKey() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggAllByRequest request = AggAllByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecBlank.getDefaultInstance()).build())
                .addGroupByColumns("Key")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggAllBy(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
    }

    @Test
    public void missingResultId() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggAllByRequest request = AggAllByRequest.newBuilder()
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecBlank.getDefaultInstance()).build())
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingSourceId() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggAllByRequest request = AggAllByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSpec(AggSpec.newBuilder().setSum(AggSpecBlank.getDefaultInstance()).build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggAllByRequest must have field source_id (2)");
    }

    @Test
    public void missingSpec() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggAllByRequest request = AggAllByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggAllByRequest must have field spec (3)");
    }

    @Test
    public void badSpecType() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggAllByRequest request = AggAllByRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggSpec must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
    }
}
