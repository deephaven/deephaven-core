//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.Exceptions;
import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class FlightSqlTicketHelper {

    public static final char TICKET_PREFIX = 'q';
    public static final String FLIGHT_DESCRIPTOR_ROUTE = "flight-sql";

    public static String toReadableString(final ByteBuffer ticket, final String logId) {
        return toReadableString(ticketToExportId(ticket, logId));
    }

    public static String toReadableString(final int exportId) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + exportId;
    }

    public static int ticketToExportId(final ByteBuffer ticket, final String logId) {
        if (ticket == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': ticket not supplied");
        }
        return ticket.order() == ByteOrder.LITTLE_ENDIAN ? ticketToExportIdInternal(ticket, logId)
                : ticketToExportIdInternal(ticket.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN), logId);
    }

    public static int ticketToExportIdInternal(final ByteBuffer ticket, final String logId) {
        if (ticket.order() != ByteOrder.LITTLE_ENDIAN) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve ticket '" + logId + "': ticket is not in LITTLE_ENDIAN order");
        }
        int pos = ticket.position();
        if (ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve ticket '" + logId + "': ticket was not provided");
        }
        if (ticket.remaining() != 5 || ticket.get(pos) != TICKET_PREFIX) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve ticket '" + logId + "': found 0x" + ByteHelper.byteBufToHex(ticket) + " (hex)");
        }
        return ticket.getInt(pos + 1);
    }

    public static Flight.Ticket exportIdToFlightTicket(int exportId) {
        final byte[] dest = new byte[5];
        dest[0] = TICKET_PREFIX;
        dest[1] = (byte) exportId;
        dest[2] = (byte) (exportId >>> 8);
        dest[3] = (byte) (exportId >>> 16);
        dest[4] = (byte) (exportId >>> 24);
        return Flight.Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }
}
