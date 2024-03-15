//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest;
import io.deephaven.proto.backplane.grpc.AjRajTablesRequest.Builder;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AsOfJoinGrpcTestBase extends GrpcTableOperationTestBase<AjRajTablesRequest> {

    public static abstract class AjTestBase extends AsOfJoinGrpcTestBase {
        @Override
        public ExportedTableCreationResponse send(AjRajTablesRequest request) {
            return channel().tableBlocking().ajTables(request);
        }

        @Test
        public void badAsOfColumnStrict() {
            final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                    .setAsOfColumn("Id<Id")
                    .build();
            assertError(request, Code.INVALID_ARGUMENT, "Unsupported operation for aj: LESS_THAN");
        }

        @Test
        public void badAsOfColumnLenient() {
            final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                    .setAsOfColumn("Id<=Id")
                    .build();
            assertError(request, Code.INVALID_ARGUMENT, "Unsupported operation for aj: LESS_THAN_EQUAL");
        }
    }

    public static abstract class RajTestBase extends AsOfJoinGrpcTestBase {
        @Override
        public ExportedTableCreationResponse send(AjRajTablesRequest request) {
            return channel().tableBlocking().rajTables(request);
        }

        @Test
        public void badAsOfColumnStrict() {
            final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                    .setAsOfColumn("Id>Id")
                    .build();
            assertError(request, Code.INVALID_ARGUMENT, "Unsupported operation for raj: GREATER_THAN");
        }

        @Test
        public void badAsOfColumnLenient() {
            final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                    .setAsOfColumn("Id>=Id")
                    .build();
            assertError(request, Code.INVALID_ARGUMENT, "Unsupported operation for raj: GREATER_THAN_EQUAL");
        }
    }

    public static class AjDefaultTest extends AjTestBase {
        @Override
        public Builder builder() {
            return AjRajTablesRequest.newBuilder().setAsOfColumn("Id");
        }
    }

    public static class AjExplicitTest extends AjTestBase {
        @Override
        public Builder builder() {
            return AjRajTablesRequest.newBuilder().setAsOfColumn("Id>=Id");
        }
    }

    public static class AjStrictTest extends AjTestBase {
        @Override
        public Builder builder() {
            return AjRajTablesRequest.newBuilder().setAsOfColumn("Id>Id");
        }
    }

    public static class RajDefaultTest extends RajTestBase {
        @Override
        public Builder builder() {
            return AjRajTablesRequest.newBuilder().setAsOfColumn("Id");
        }
    }

    public static class RajExplicitTest extends RajTestBase {
        @Override
        public Builder builder() {
            return AjRajTablesRequest.newBuilder().setAsOfColumn("Id<=Id");
        }
    }

    public static class RajStrictTest extends RajTestBase {
        @Override
        public Builder builder() {
            return AjRajTablesRequest.newBuilder().setAsOfColumn("Id<Id");
        }
    }

    public abstract Builder builder();

    @Test
    public void missingResultId() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .clearResultId()
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "No result ticket supplied");
    }

    @Test
    public void missingLeftId() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .clearLeftId()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AjRajTablesRequest must have field left_id (2)");
    }

    @Test
    public void missingRightId() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .clearRightId()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AjRajTablesRequest must have field right_id (3)");
    }


    @Test
    public void missingAsOfColumn() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .clearAsOfColumn()
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AjRajTablesRequest must have field as_of_column (5)");
    }

    @Test
    public void badAsOfColumn() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .setAsOfColumn("not a column")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Input expression not a column does not match expected pattern");
    }

    @Test
    public void badExactMatches() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .addExactMatchColumns("bad,exact,match")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name");
    }

    @Test
    public void badColumnsToAdd() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .addColumnsToAdd("this,is,bad")
                .build();
        assertError(request, Code.INVALID_ARGUMENT, "Invalid column name");
    }

    @Test
    public void unknownField() {
        final AjRajTablesRequest request = AjRajTablesRequest.newBuilder(prototype())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(9999, Field.newBuilder().addFixed32(32).build())
                        .build())
                .build();
        assertError(request, Code.INVALID_ARGUMENT,
                "io.deephaven.proto.backplane.grpc.AjRajTablesRequest has unknown field(s)");
    }

    @Test
    public void requestStatic() {
        final AjRajTablesRequest request = prototype();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(1);
        } finally {
            release(response);
        }
    }

    AjRajTablesRequest prototype() {
        final TableReference idTable = ref(TableTools.emptyTable(1).view("Id=ii"));
        return builder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setLeftId(idTable)
                .setRightId(idTable)
                .addColumnsToAdd("Id2=Id")
                .build();
    }
}
