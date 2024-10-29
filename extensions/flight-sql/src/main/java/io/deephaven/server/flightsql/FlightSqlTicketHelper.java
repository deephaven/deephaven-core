//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import org.apache.arrow.flight.impl.Flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;

import java.nio.ByteBuffer;

final class FlightSqlTicketHelper {

    public static final char TICKET_PREFIX = 'q';

    private static final ByteString PREFIX = ByteString.copyFrom(new byte[] {(byte) TICKET_PREFIX});

    public static String toReadableString(final ByteBuffer ticket, final String logId) {
        final Any any = unpackTicket(ticket, logId);
        // We don't necessarily want to print out the full protobuf; this will at least give some more logging info on
        // the type of the ticket.
        return any.getTypeUrl();
    }

    public static Any unpackTicket(ByteBuffer ticket, final String logId) {
        ticket = ticket.slice();
        if (ticket.get() != TICKET_PREFIX) {
            // If we get here, it means there is an error with FlightSqlResolver.ticketRoute /
            // io.deephaven.server.session.TicketRouter.getResolver
            throw new IllegalStateException("Could not resolve FlightSQL ticket '" + logId + "': invalid prefix");
        }
        try {
            return Any.parseFrom(ticket);
        } catch (InvalidProtocolBufferException e) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve Flight SQL ticket '" + logId + "': invalid payload");
        }
    }

    public static Ticket ticketFor(CommandGetCatalogs command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(CommandGetDbSchemas command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(CommandGetTableTypes command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(CommandGetImportedKeys command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(CommandGetExportedKeys command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(CommandGetPrimaryKeys command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(CommandGetTables command) {
        return packedTicket(command);
    }

    public static Ticket ticketFor(TicketStatementQuery query) {
        return packedTicket(query);
    }

    private static Ticket packedTicket(Message message) {
        return Ticket.newBuilder().setTicket(PREFIX.concat(Any.pack(message).toByteString())).build();
    }
}
