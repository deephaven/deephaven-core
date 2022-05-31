package io.deephaven.extensions.barrage.util;

import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;

public class ExportUtil {
    public static ExportedTableCreationResponse buildTableCreationResponse(Ticket ticket, Table table) {
        return buildTableCreationResponse(TableReference.newBuilder().setTicket(ticket).build(), table);
    }

    public static ExportedTableCreationResponse buildTableCreationResponse(TableReference tableRef, Table table) {
        return ExportedTableCreationResponse.newBuilder()
                .setSuccess(true)
                .setResultId(tableRef)
                .setIsStatic(!table.isRefreshing())
                .setSize(table.size())
                .setSchemaHeader(BarrageUtil.schemaBytesFromTable(table))
                .build();
    }
}
