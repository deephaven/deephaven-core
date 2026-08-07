//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortDescriptor.SortDirection;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SortTableGrpcTest extends GrpcTableOperationTestBase<SortTableRequest> {

    @Override
    public ExportedTableCreationResponse send(SortTableRequest request) {
        return channel().tableBlocking().sort(request);
    }

    @Test
    public void absoluteSortExistingColumn() {
        final TableReference source = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SortTableRequest request = SortTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(source)
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("Id")
                        .setIsAbsolute(true)
                        .setDirection(SortDirection.ASCENDING)
                        .build())
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(5);
        } finally {
            release(response);
        }
    }

    @Test
    public void absoluteSortMissingColumn() {
        final TableReference source = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SortTableRequest request = SortTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(source)
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("DoesNotExist")
                        .setIsAbsolute(true)
                        .setDirection(SortDirection.ASCENDING)
                        .build())
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "column(s) not found: DoesNotExist");
    }

    @Test
    public void nonAbsoluteSortMissingColumn() {
        final TableReference source = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SortTableRequest request = SortTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(source)
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("DoesNotExist")
                        .setDirection(SortDirection.DESCENDING)
                        .build())
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "column(s) not found: DoesNotExist");
    }

    @Test
    public void multipleMissingColumns() {
        final TableReference source = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SortTableRequest request = SortTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(source)
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("MissingA")
                        .setDirection(SortDirection.ASCENDING)
                        .build())
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("Id")
                        .setDirection(SortDirection.ASCENDING)
                        .build())
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("MissingB")
                        .setIsAbsolute(true)
                        .setDirection(SortDirection.DESCENDING)
                        .build())
                .build();
        assertError(request, Code.FAILED_PRECONDITION, "column(s) not found: MissingA, MissingB");
    }

    @Test
    public void reverseIgnoresColumnName() {
        final TableReference source = ref(TableTools.emptyTable(5).view("Id=ii"));
        final SortTableRequest request = SortTableRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                .setSourceId(source)
                .addSorts(SortDescriptor.newBuilder()
                        .setColumnName("DoesNotExist")
                        .setDirection(SortDirection.REVERSE)
                        .build())
                .build();
        final ExportedTableCreationResponse response = send(request);
        try {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(5);
        } finally {
            release(response);
        }
    }
}
