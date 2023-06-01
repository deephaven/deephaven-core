/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request;
import io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request.AsOfRule;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AsOfJoinGrpcTest extends GrpcTableOperationTestBase<AsOfJoinTables2Request> {

    @Override
    public ExportedTableCreationResponse send(AsOfJoinTables2Request request) {
        return channel().tableBlocking().asOfJoinTables2(request);
    }

    @Test
    public void missingResultId() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .clearResultId()
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingLeftId() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .clearLeftId()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request must have field left_id (2)");
    }

    @Test
    public void missingRightId() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .clearRightId()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request must have field right_id (3)");
    }

    @Test
    public void missingLeftColumn() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .clearLeftColumn()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request must have field left_column (5)");
    }

    @Test
    public void missingRightColumn() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .clearRightColumn()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request must have field right_column (7)");
    }

    @Test
    public void badLeftColumn() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setLeftColumn("not a column")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name \"not a column\"");
    }

    @Test
    public void badRightColumn() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRightColumn("not a column")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name \"not a column\"");
    }

    @Test
    public void badExactMatches() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .addExactMatchColumns("bad,exact,match")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name");
    }

    @Test
    public void unspecifiedRule() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRule(AsOfRule.UNSPECIFIED)
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request must have field rule (6)");
    }

    @Test
    public void unrecognizedRule() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRuleValue(999)
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Unexpected rule: UNRECOGNIZED");
    }

    @Test
    public void badColumnsToAdd() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .addColumnsToAdd("this,is,bad")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name");
    }

    @Test
    public void unknownField() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AsOfJoinTables2Request has unknown field(s)");
    }

    @Test
    public void asOfJoinGeqStatic() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRule(AsOfRule.GEQ)
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().asOfJoinTables2(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void asOfJoinGtStatic() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRule(AsOfRule.GT)
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().asOfJoinTables2(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void asOfJoinLeqStatic() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRule(AsOfRule.LEQ)
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().asOfJoinTables2(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    @Test
    public void asOfJoinLtStatic() {
        final AsOfJoinTables2Request request = AsOfJoinTables2Request.newBuilder(prototype())
                .setRule(AsOfRule.LT)
                .build();
        final ExportedTableCreationResponse response = channel().tableBlocking().asOfJoinTables2(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    private AsOfJoinTables2Request prototype() {
        final TableReference idTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        return AsOfJoinTables2Request.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(idTable)
                .setRightId(idTable)
                .setLeftColumn("Id")
                .setRule(AsOfRule.GEQ)
                .setRightColumn("Id")
                .addColumnsToAdd("Id2=Id")
                .build();
    }
}
