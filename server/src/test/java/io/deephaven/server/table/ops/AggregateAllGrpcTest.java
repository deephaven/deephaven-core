package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSum;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique;
import io.deephaven.proto.backplane.grpc.AggregateAllRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.NullValue;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregateAllGrpcTest extends GrpcTableOperationTestBase<AggregateAllRequest> {

    @Override
    public ExportedTableCreationResponse send(AggregateAllRequest request) {
        return channel().tableBlocking().aggregateAll(request);
    }

    @Test
    public void aggregateAll() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregateAll(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }

    @Test
    public void aggregateAllKey() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                .addGroupByColumns("Key")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregateAll(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
    }

    @Test
    public void missingResultId() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingSourceId() {
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggregateAllRequest must have field source_id (2)");
    }

    @Test
    public void missingSpec() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggregateAllRequest must have field spec (3)");
    }

    @Test
    public void badSpecType() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggSpec must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
    }

    @Test
    public void emptySourceId() {
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(TableReference.newBuilder().build())
                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
    }

    @Test
    public void unknownField() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggregateAllRequest has unknown field(s)");
    }

    @Test
    public void name() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateAllRequest request = AggregateAllRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .setSpec(AggSpec.newBuilder().setUnique(AggSpecUnique.newBuilder()
                        .setIncludeNulls(false)
                        .setNonUniqueSentinel(
                                AggSpecNonUniqueSentinel.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .build()).build())
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregateAll(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }
}
