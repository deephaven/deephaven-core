package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecBlank;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecCountDistinct;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCount;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationPartition;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationRowKey;
import io.deephaven.proto.backplane.grpc.AggregationRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregationGrpcTest extends GrpcTableOperationTestBase<AggregationRequest> {

    @Override
    public ExportedTableCreationResponse send(AggregationRequest request) {
        return channel().tableBlocking().aggregate(request);
    }

    @Test
    public void aggregationColumns() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setSourceId(ref(emptyTable))
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
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationCount() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setSourceId(ref(emptyTable))
                    .addAggregations(Aggregation.newBuilder()
                            .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                            .build())
                    .build();
            final ExportedTableCreationResponse response = channel().tableBlocking().aggregate(request);
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationPartition() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setSourceId(ref(emptyTable))
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
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationRowKeys() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setSourceId(ref(emptyTable))
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
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationColumnsMultiple() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii", "J=ii % 3"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setSourceId(ref(emptyTable))
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
                                            .setSum(AggSpecBlank.getDefaultInstance())
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
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void missingResultId() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .addAggregations(Aggregation.newBuilder()
                            .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                            .build())
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.AggregationRequest must have field source_id (2)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void missingSourceId() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setSourceId(ref(emptyTable))
                    .addAggregations(Aggregation.newBuilder()
                            .setCount(AggregationCount.newBuilder().setColumnName("Count").build())
                            .build())
                    .build();
            assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void emptyAggregations() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .setSourceId(ref(emptyTable))
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.AggregationRequest must have at least one aggregations (5)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void badType() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .addAggregations(Aggregation.getDefaultInstance())
                    .setSourceId(ref(emptyTable))
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.Aggregation must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationColumnsMissingSpec() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .addAggregations(Aggregation.newBuilder()
                            .setColumns(AggregationColumns.newBuilder()
                                    .addMatchPairs("I")
                                    .build())
                            .build())
                    .setSourceId(ref(emptyTable))
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have field spec (1)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationColumnsMissingPair() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .addAggregations(Aggregation.newBuilder()
                            .setColumns(AggregationColumns.newBuilder()
                                    .setSpec(AggSpec.newBuilder().setSum(AggSpecBlank.getDefaultInstance()).build())
                                    .build())
                            .build())
                    .setSourceId(ref(emptyTable))
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns must have at least one match_pairs (2)");
        } finally {
            emptyTable.cancel();
        }
    }

    @Test
    public void aggregationColumnsBadType() {
        final ExportObject<Table> emptyTable = authenticatedSessionState()
                .newServerSideExport(TableTools.emptyTable(100).view("Key=ii % 2", "I=ii"));
        try {
            final AggregationRequest request = AggregationRequest.newBuilder()
                    .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                    .addAggregations(Aggregation.newBuilder()
                            .setColumns(AggregationColumns.newBuilder()
                                    .setSpec(AggSpec.newBuilder().build())
                                    .addMatchPairs("I")
                                    .build())
                            .build())
                    .setSourceId(ref(emptyTable))
                    .build();
            assertError(request, Code.INVALID_ARGUMENT,
                    "io.deephaven.proto.backplane.grpc.AggSpec must have oneof type. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
        } finally {
            emptyTable.cancel();
        }
    }
}
