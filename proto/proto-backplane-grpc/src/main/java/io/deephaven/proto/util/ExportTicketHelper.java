/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.proto.util;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

public class ExportTicketHelper {
    public static final byte TICKET_PREFIX = 'e';

    public static final String FLIGHT_DESCRIPTOR_ROUTE = "export";

    /**
     * Convenience method to convert from export id to {@link Ticket}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Ticket wrapExportIdInTicket(int exportId) {
        final byte[] dest = exportIdToBytes(exportId);
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }

    /**
     * Convenience method to convert from export id to a ticket {@link TableReference}.
     *
     * @param exportId the export id
     * @return a table reference
     */
    public static TableReference tableReference(int exportId) {
        return TableReference.newBuilder().setTicket(wrapExportIdInTicket(exportId)).build();
    }

    /**
     * Convenience method to convert from {@link Ticket} to export id.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int export id in
     * little-endian.
     *
     * @param ticket the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the export id that the Ticket wraps
     */
    public static int ticketToExportId(final Ticket ticket, final String logId) {
        return ticketToExportIdInternal(
                ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), logId);
    }

    /**
     * Convenience method to convert from {@link ByteBuffer} to export id. Most efficient when {@code ticket} is
     * {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int export id in
     * little-endian.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the export id that the Ticket wraps
     */
    public static int ticketToExportId(final ByteBuffer ticket, final String logId) {
        if (ticket == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': ticket not supplied");
        }
        return ticket.order() == ByteOrder.LITTLE_ENDIAN ? ticketToExportIdInternal(ticket, logId)
                : ticketToExportIdInternal(ticket.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN), logId);
    }

    /**
     * Convenience method to convert from {@link ByteBuffer} to export ticket. Most efficient when {@code ticket} is
     * {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int export id in
     * little-endian.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static Ticket wrapExportIdInTicket(final ByteBuffer ticket) {
        final ByteBuffer lebb = ticket.order() == ByteOrder.LITTLE_ENDIAN ? ticket
                : ticket.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(lebb)).build();
    }

    /**
     * Convenience method to create a human readable string from the flight ticket.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a log-friendly string
     */
    public static String toReadableString(final Ticket ticket, final String logId) {
        return toReadableString(
                ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), logId);
    }

    /**
     * Convenience method to create a human readable string from a table reference.
     *
     * @param tableReference the table reference
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a log-friendly string
     */
    public static String toReadableString(final TableReference tableReference, final String logId) {
        switch (tableReference.getRefCase()) {
            case TICKET:
                return toReadableString(tableReference.getTicket(), logId);
            case BATCH_OFFSET:
                return String.format("batchOffset[%d]", tableReference.getBatchOffset());
            default:
                throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Could not resolve '" + logId + "': unexpected TableReference type '"
                                + tableReference.getRefCase() + "'");
        }
    }

    /**
     * Convenience method to create a human readable string from the flight ticket (as ByteBuffer). Most efficient when
     * {@code ticket} is {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a log-friendly string
     */
    public static String toReadableString(final ByteBuffer ticket, final String logId) {
        return toReadableString(ticketToExportId(ticket, logId));
    }

    /**
     * Convenience method to create a human readable string for the export ID.
     *
     * @param exportId the export ID
     * @return a log-friendly string
     */
    public static String toReadableString(final int exportId) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + exportId;
    }

    /**
     * Convenience method to create the flight descriptor path for the export ID.
     *
     * @param exportId the export ID
     * @return the path
     */
    public static List<String> exportIdToPath(int exportId) {
        return Arrays.asList(FLIGHT_DESCRIPTOR_ROUTE, Integer.toString(exportId));
    }

    /**
     * Convenience method to create the flight ticket bytes for the export ID.
     *
     * @param exportId the export ID
     * @return the ticket bytes
     */
    public static byte[] exportIdToBytes(int exportId) {
        final byte[] dest = new byte[5];
        dest[0] = TICKET_PREFIX;
        dest[1] = (byte) exportId;
        dest[2] = (byte) (exportId >>> 8);
        dest[3] = (byte) (exportId >>> 16);
        dest[4] = (byte) (exportId >>> 24);
        return dest;
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
}
