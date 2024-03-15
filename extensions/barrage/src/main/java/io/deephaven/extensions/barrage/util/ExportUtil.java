//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.UncoalescedTable;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;

public class ExportUtil {
    public static ExportedTableCreationResponse buildTableCreationResponse(Ticket ticket, Table table) {
        return buildTableCreationResponse(TableReference.newBuilder().setTicket(ticket).build(), table);
    }

    public static ExportedTableCreationResponse buildTableCreationResponse(TableReference tableRef, Table table) {
        final long size;
        if (table instanceof UncoalescedTable) {
            size = Long.MIN_VALUE;
        } else {
            size = table.size();
        }
        return ExportedTableCreationResponse.newBuilder()
                .setSuccess(true)
                .setResultId(tableRef)
                .setIsStatic(!table.isRefreshing())
                .setSize(size)
                .setSchemaHeader(BarrageUtil.schemaBytesFromTable(table))
                .build();
    }
}
