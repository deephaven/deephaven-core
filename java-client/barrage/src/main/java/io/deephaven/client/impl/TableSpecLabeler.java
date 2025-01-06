//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.proto.util.ApplicationTicketHelper;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.proto.util.ScopeTicketHelper;
import io.deephaven.proto.util.SharedTicketHelper;
import io.deephaven.qst.table.TableLabelVisitor;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import org.apache.commons.codec.binary.Hex;

import java.nio.ByteBuffer;

final class TableSpecLabeler extends TableLabelVisitor {
    private static final TableSpecLabeler INSTANCE = new TableSpecLabeler();

    public static String of(TableSpec tableSpec) {
        return tableSpec.walk(INSTANCE);
    }

    private TableSpecLabeler() {}

    static String nameForTableTicket(TicketTable table) {
        byte[] ticket = table.ticket();
        if (ticket.length == 0) {
            return "ticketTable(EMPTY)";
        }

        // We'll try our best to decode the ticket, but it's not guaranteed to be a well-known ticket route.
        try {
            switch (ticket[0]) {
                case 'a':
                    return ApplicationTicketHelper.toReadableString(ticket);
                case 's':
                    return ScopeTicketHelper.toReadableString(ticket);
                case 'e':
                    return ExportTicketHelper.toReadableString(ByteBuffer.wrap(ticket), "TicketTable");
                case 'h':
                    return SharedTicketHelper.toReadableString(ticket);
                default:
                    break;
            }
        } catch (Exception err) {
            // ignore - let's just return the hex representation
        }

        return "ticketTable(0x" + Hex.encodeHexString(ticket) + ")";
    }

    @Override
    public String visit(TicketTable ticketTable) {
        return nameForTableTicket(ticketTable);
    }
}
