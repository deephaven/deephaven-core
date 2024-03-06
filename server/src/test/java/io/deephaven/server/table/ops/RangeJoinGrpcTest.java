//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecGroup;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest.RangeEndRule;
import io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest.RangeStartRule;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeJoinGrpcTest extends GrpcTableOperationTestBase<RangeJoinTablesRequest> {

    @Override
    public ExportedTableCreationResponse send(RangeJoinTablesRequest request) {
        return channel().tableBlocking().rangeJoinTables(request);
    }

    @Test
    public void rangeJoinStatic() {
        final RangeJoinTablesRequest request = prototype();
        final ExportedTableCreationResponse response = channel().tableBlocking().rangeJoinTables(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void rangeJoinStaticStringMatch() {
        final RangeJoinTablesRequest request = prototypeStringMatch();
        final ExportedTableCreationResponse response = channel().tableBlocking().rangeJoinTables(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void stringMatchAndDetailsProvided() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setRangeMatch("Lower <= X <= Upper")
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "If `range_match` is provided, range details should remain empty.");
    }

    @Test
    public void missingResultId() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearResultId()
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingLeftId() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearLeftId()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field left_id (2)");
    }

    @Test
    public void missingRightId() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearRightId()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field right_id (3)");
    }

    @Test
    public void missingLeftStartColumn() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearLeftStartColumn()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field left_start_column (5)");
    }

    @Test
    public void missingRangeStart() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearRangeStartRule()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field range_start_rule (6)");
    }

    @Test
    public void missingRightRangeColumn() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearRightRangeColumn()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field right_range_column (7)");
    }

    @Test
    public void missingRangeEnd() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearRangeEndRule()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field range_end_rule (8)");
    }

    @Test
    public void missingLeftEndColumn() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearLeftEndColumn()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have field left_end_column (9)");
    }

    @Test
    public void missingAggregations() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .clearAggregations()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest must have at least one aggregations (10)");
    }

    @Test
    public void unknownFields() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.RangeJoinTablesRequest has unknown field(s)");
    }

    @Test
    public void badLeftStartColumn() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setLeftStartColumn("not a column name")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name \"not a column name\"");
    }

    @Test
    public void badRangeStartRule() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setRangeStartRuleValue(999)
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Unrecognized range start rule UNRECOGNIZED for range join");
    }

    @Test
    public void badRightRangeColumn() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setRightRangeColumn("not a column name")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name \"not a column name\"");
    }

    @Test
    public void badRangeEndRule() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setRangeEndRuleValue(999)
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Unrecognized range end rule UNRECOGNIZED for range join");
    }

    @Test
    public void badLeftEndColumn() {
        final RangeJoinTablesRequest request = RangeJoinTablesRequest.newBuilder(prototype())
                .setLeftEndColumn("not a column name")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name \"not a column name\"");
    }

    private RangeJoinTablesRequest prototype() {
        final TableReference t1 = ref(TableTools.emptyTable(1).view("Lower=ii", "Upper=ii"));
        final TableReference t2 = ref(TableTools.emptyTable(1).view("X=ii", "Y=ii"));
        return RangeJoinTablesRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(t1)
                .setRightId(t2)
                .setLeftStartColumn("Lower")
                .setRangeStartRule(RangeStartRule.LESS_THAN_OR_EQUAL)
                .setRightRangeColumn("X")
                .setRangeEndRule(RangeEndRule.GREATER_THAN_OR_EQUAL)
                .setLeftEndColumn("Upper")
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder()
                                        .setGroup(AggSpecGroup.getDefaultInstance())
                                        .build())
                                .addMatchPairs("Y")
                                .build())
                        .build())
                .build();
    }

    private RangeJoinTablesRequest prototypeStringMatch() {
        final TableReference t1 = ref(TableTools.emptyTable(1).view("Lower=ii", "Upper=ii"));
        final TableReference t2 = ref(TableTools.emptyTable(1).view("X=ii", "Y=ii"));
        return RangeJoinTablesRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(t1)
                .setRightId(t2)
                .setRangeMatch("Lower <= X <= Upper")
                .addAggregations(Aggregation.newBuilder()
                        .setColumns(AggregationColumns.newBuilder()
                                .setSpec(AggSpec.newBuilder()
                                        .setGroup(AggSpecGroup.getDefaultInstance())
                                        .build())
                                .addMatchPairs("Y")
                                .build())
                        .build())
                .build();
    }
}
