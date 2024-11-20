//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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

import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_CATALOGS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_DB_SCHEMAS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_EXPORTED_KEYS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_IMPORTED_KEYS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_PRIMARY_KEYS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_TABLES_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlSharedConstants.COMMAND_GET_TABLE_TYPES_TYPE_URL;

final class FlightSqlTicketHelper {

    public static final char TICKET_PREFIX = 'q';

    // This is a server-implementation detail, but happens to be the same scheme that Flight SQL
    // org.apache.arrow.flight.sql.FlightSqlProducer uses
    @VisibleForTesting
    static final String TICKET_STATEMENT_QUERY_TYPE_URL =
            FlightSqlSharedConstants.FLIGHT_SQL_TYPE_PREFIX + "TicketStatementQuery";

    private static final ByteString PREFIX = ByteString.copyFrom(new byte[] {(byte) TICKET_PREFIX});

    interface TicketVisitor<T> {

        // These ticket objects could be anything we want; they don't _have_ to be the protobuf objects. But, they are
        // convenient as they already contain the information needed to act on the ticket.

        T visit(CommandGetCatalogs ticket);

        T visit(CommandGetDbSchemas ticket);

        T visit(CommandGetTableTypes ticket);

        T visit(CommandGetImportedKeys ticket);

        T visit(CommandGetExportedKeys ticket);

        T visit(CommandGetPrimaryKeys ticket);

        T visit(CommandGetTables ticket);

        T visit(TicketStatementQuery ticket);
    }

    public static TicketVisitor<Ticket> ticketCreator() {
        return TicketCreator.INSTANCE;
    }

    public static String toReadableString(final ByteBuffer ticket, final String logId) {
        final Any any = partialUnpackTicket(ticket, logId);
        // We don't necessarily want to print out the full protobuf; this will at least give some more logging info on
        // the type of the ticket.
        return any.getTypeUrl();
    }

    public static <T> T visit(ByteBuffer ticket, TicketVisitor<T> visitor, String logId) {
        return visit(partialUnpackTicket(ticket, logId), visitor, logId);
    }

    private static Any partialUnpackTicket(ByteBuffer ticket, final String logId) {
        ticket = ticket.slice();
        // If false, it means there is an error with FlightSqlResolver.ticketRoute / TicketRouter.getResolver
        Assert.eq(ticket.get(), "ticket.get()", TICKET_PREFIX, "TICKET_PREFIX");
        try {
            return Any.parseFrom(ticket);
        } catch (InvalidProtocolBufferException e) {
            throw invalidTicket(logId);
        }
    }

    private static <T> T visit(Any ticket, TicketVisitor<T> visitor, String logId) {
        switch (ticket.getTypeUrl()) {
            case TICKET_STATEMENT_QUERY_TYPE_URL:
                return visitor.visit(unpack(ticket, TicketStatementQuery.class, logId));
            case COMMAND_GET_TABLES_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetTables.class, logId));
            case COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetTableTypes.class, logId));
            case COMMAND_GET_CATALOGS_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetCatalogs.class, logId));
            case COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetDbSchemas.class, logId));
            case COMMAND_GET_PRIMARY_KEYS_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetPrimaryKeys.class, logId));
            case COMMAND_GET_IMPORTED_KEYS_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetImportedKeys.class, logId));
            case COMMAND_GET_EXPORTED_KEYS_TYPE_URL:
                return visitor.visit(unpack(ticket, CommandGetExportedKeys.class, logId));
        }
        throw invalidTicket(logId);
    }

    private enum TicketCreator implements TicketVisitor<Ticket> {
        INSTANCE;

        @Override
        public Ticket visit(CommandGetCatalogs ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(CommandGetDbSchemas ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(CommandGetTableTypes ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(CommandGetImportedKeys ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(CommandGetExportedKeys ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(CommandGetPrimaryKeys ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(CommandGetTables ticket) {
            return packedTicket(ticket);
        }

        @Override
        public Ticket visit(TicketStatementQuery ticket) {
            return packedTicket(ticket);
        }

        private static Ticket packedTicket(Message message) {
            // Note: this is _similar_ to how the Flight SQL example server implementation constructs tickets using
            // Any#pack; the big difference is that all DH tickets must (currently) be uniquely route-able based on the
            // first byte of the ticket.
            return Ticket.newBuilder().setTicket(PREFIX.concat(Any.pack(message).toByteString())).build();
        }
    }

    private static StatusRuntimeException invalidTicket(String logId) {
        return FlightSqlErrorHelper.error(Status.Code.FAILED_PRECONDITION, String.format("Invalid ticket, %s", logId));
    }

    private static <T extends Message> T unpack(Any ticket, Class<T> clazz, String logId) {
        try {
            return ticket.unpack(clazz);
        } catch (InvalidProtocolBufferException e) {
            throw invalidTicket(logId);
        }
    }
}
