//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecCountDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSum;
import io.deephaven.proto.backplane.grpc.AggregateRequest;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCount;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationPartition;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationRowKey;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregateGrpcTest extends GrpcTableOperationTestBase<AggregateRequest> {

    @Override
    public ExportedTableCreationResponse send(AggregateRequest request) {
        return channel().tableBlocking().aggregate(request);
    }

    @Test
    public void aggregationColumns() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder()
                                        .setCountDistinct(AggSpecCountDistinct.getDefaultInstance())
                                        .build())
                                .addMatchPairs("I")
                                .build())
                        .build())
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregate(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }

    @Test
    public void aggregationCount() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                        .build())
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregate(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(1);
    }

    @Test
    public void aggregationPartition() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setPartition(AggregationPartition.newBuilder()
                                .setColumnName("I")
                                .build())
                        .build())
                .addGroupByColumns("Key")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregate(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
    }

    @Test
    public void aggregationRowKeys() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setFirstRowKey(AggregationRowKey.newBuilder().setColumnName("FirstRowKey").build())
                        .build())
                .addAggregations(Aggregation.newBuilder()
                        .setLastRowKey(AggregationRowKey.newBuilder().setColumnName("LastRowKey").build())
                        .build())
                .addGroupByColumns("Key")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregate(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
    }

    @Test
    public void aggregationColumnsMultiple() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii", "J=ii % 3"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder()
                                        .setCountDistinct(AggSpecCountDistinct.getDefaultInstance())
                                        .build())
                                .addMatchPairs("CountDistinctI=I")
                                .addMatchPairs("CountDistinctJ=J")
                                .build())
                        .build())
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder()
                                        .setSum(AggSpecSum.getDefaultInstance())
                                        .build())
                                .addMatchPairs("SumI=I")
                                .addMatchPairs("SumJ=J")
                                .build())
                        .build())
                .addGroupByColumns("Key")
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().aggregate(request);
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getIsStatic()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
    }

    @Test
    public void missingResultId() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                        .build())
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingSourceId() {
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addAggregations(Aggregation.newBuilder()
                        .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggregateRequest must have field source_id (2)");
    }

    @Test
    public void emptySourceId() {
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(TableReference.newBuilder().build())
                .addAggregations(Aggregation.newBuilder()
                        .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.TableReference must have oneof ref. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
    }

    @Test
    public void emptyAggregations() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggregateRequest must have at least one aggregations (5)");
    }

    @Test
    public void badType() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addAggregations(Aggregation.getDefaultInstance())
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.Aggregation must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
    }

    @Test
    public void aggregationColumnsMissingSpec() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .addMatchPairs("I")
                                .build())
                        .build())
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have field spec (1)");
    }

    @Test
    public void aggregationColumnsMissingPair() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder().setSum(AggSpecSum.getDefaultInstance()).build())
                                .build())
                        .build())
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have at least one match_pairs (2)");
    }

    @Test
    public void aggregationColumnsBadType() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder().build())
                                .addMatchPairs("I")
                                .build())
                        .build())
                .setSourceId(ref)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggSpec must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
    }

    @Test
    public void unknownField() {
        final TableReference ref = ref(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        final AggregateRequest request = AggregateRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(ref)
                .addAggregations(Aggregation.newBuilder()
                        .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                        .build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AggregateRequest has unknown field(s)");
    }
}
