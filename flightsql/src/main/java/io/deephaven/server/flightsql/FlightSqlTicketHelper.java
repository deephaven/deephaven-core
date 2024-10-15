//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.Exceptions;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class FlightSqlTicketHelper {

    public static final char TICKET_PREFIX = 'q';
    public static final String FLIGHT_DESCRIPTOR_ROUTE = "flight-sql";

    private static final ByteString PREFIX = ByteString.copyFrom(new byte[] {(byte) TICKET_PREFIX});

    public static String toReadableString(final ByteBuffer ticket, final String logId) {
        // TODO
        final Any any = unpackTicket(ticket, logId);
        return any.toString();
        // return "TODO";
        // return toReadableString(ticketToExportId(ticket, logId));
    }

    public static Any unpackTicket(ByteBuffer ticket, final String logId) {
        ticket = ticket.slice();
        if (ticket.get() != TICKET_PREFIX) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve FlightSQL ticket '" + logId + "': invalid prefix");
        }
        try {
            return Any.parseFrom(ticket);
        } catch (InvalidProtocolBufferException e) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve FlightSQL ticket '" + logId + "': invalid payload");
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

    public static Flight.Ticket ticketFor(CommandGetTables command) {
        return packedTicket(command);
    }

    public static Flight.Ticket ticketFor(TicketStatementQuery query) {
        return packedTicket(query);
    }

    private static Flight.Ticket packedTicket(Message message) {
        return Ticket.newBuilder().setTicket(PREFIX.concat(Any.pack(message).toByteString())).build();
    }
}
