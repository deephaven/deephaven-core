/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.util;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ExportTicketHelper {
    public static final byte TICKET_PREFIX = 'e';

    public static final String FLIGHT_DESCRIPTOR_ROUTE = "export";

    /**
     * Convenience method to convert from export id to {@link Flight.Ticket}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Flight.Ticket exportIdToArrowTicket(int exportId) {
        final byte[] dest = exportIdToBytes(exportId);
        return Flight.Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }

    /**
     * Convenience method to convert from export id to {@link Flight.Ticket}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Ticket exportIdToTicket(int exportId) {
        final byte[] dest = exportIdToBytes(exportId);
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }

    /**
     * Convenience method to convert from export id to {@link Flight.FlightDescriptor}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Flight.FlightDescriptor exportIdToDescriptor(int exportId) {
        return Flight.FlightDescriptor.newBuilder()
            .setType(Flight.FlightDescriptor.DescriptorType.PATH).addPath(FLIGHT_DESCRIPTOR_ROUTE)
            .addPath(Integer.toString(exportId)).build();
    }

    /**
     * Convenience method to convert from {@link Flight.Ticket} to export id.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed
     * int export id in little-endian.
     *
     * @param ticket the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static int ticketToExportId(final Ticket ticket) {
        return ticketToExportIdInternal(
            ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Convenience method to convert from {@link Flight.Ticket} to export id.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed
     * int export id in little-endian.
     *
     * @param ticket the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static int ticketToExportId(final Flight.Ticket ticket) {
        return ticketToExportIdInternal(
            ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Convenience method to convert from {@link ByteBuffer} to export id. Most efficient when
     * {@code ticket} is {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed
     * int export id in little-endian.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static int ticketToExportId(final ByteBuffer ticket) {
        if (ticket == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Ticket not supplied");
        }
        return ticket.order() == ByteOrder.LITTLE_ENDIAN ? ticketToExportIdInternal(ticket)
            : ticketToExportIdInternal(ticket.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN));
    }


    /**
     * Convenience method to convert from {@link ByteBuffer} to export ticket. Most efficient when
     * {@code ticket} is {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed
     * int export id in little-endian.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static Ticket exportIdToTicket(final ByteBuffer ticket) {
        final ByteBuffer lebb = ticket.order() == ByteOrder.LITTLE_ENDIAN ? ticket
            : ticket.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(lebb)).build();
    }

    /**
     * Convenience method to convert from {@link Flight.FlightDescriptor} to export id.
     *
     * <p>
     * Descriptor must be a path.
     *
     * @param descriptor the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static int descriptorToExportId(final Flight.FlightDescriptor descriptor) {
        if (descriptor == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Descriptor not supplied");
        }
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Cannot parse descriptor: not a path");
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Cannot parse descriptor: unexpected path length (found: "
                    + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }

        try {
            return Integer.parseInt(descriptor.getPath(1));
        } catch (final NumberFormatException nfe) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Cannot parse descriptor: export id not numeric (found: "
                    + TicketRouterHelper.getLogNameFor(descriptor) + ")");
        }
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Ticket ticket) {
        return exportIdToDescriptor(ticketToExportId(ticket));
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @return a flight descriptor that represents the ticket
     */
    // TODO #412 use this or remove it
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket) {
        return exportIdToDescriptor(ticketToExportId(ticket));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToArrowTicket(final Flight.FlightDescriptor descriptor) {
        return exportIdToArrowTicket(descriptorToExportId(descriptor));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @return a flight ticket that represents the descriptor
     */
    // TODO #412 use this or remove it
    public static Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor) {
        return exportIdToTicket(descriptorToExportId(descriptor));
    }

    /**
     * Convenience method to create a human readable string from the flight ticket.
     *
     * @param ticket the ticket to convert
     * @return a log-friendly string
     */
    public static String toReadableString(final Ticket ticket) {
        return toReadableString(
            ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Convenience method to create a human readable string from the flight ticket (as ByteBuffer).
     * Most efficient when {@code ticket} is {@link ByteOrder#LITTLE_ENDIAN}.
     *
     * <p>
     * Does not consume the {@code ticket}.
     *
     * @param ticket the ticket to convert
     * @return a log-friendly string
     */
    public static String toReadableString(final ByteBuffer ticket) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + ticketToExportId(ticket);
    }

    private static byte[] exportIdToBytes(int exportId) {
        final byte[] dest = new byte[5];
        dest[0] = TICKET_PREFIX;
        dest[1] = (byte) exportId;
        dest[2] = (byte) (exportId >>> 8);
        dest[3] = (byte) (exportId >>> 16);
        dest[4] = (byte) (exportId >>> 24);
        return dest;
    }

    private static String byteBufToHex(final ByteBuffer ticket) {
        StringBuilder sb = new StringBuilder();
        for (int i = ticket.position(); i < ticket.limit(); ++i) {
            sb.append(String.format("%02x", ticket.get(i)));
        }
        return sb.toString();
    }

    private static int ticketToExportIdInternal(final ByteBuffer ticket) {
        if (ticket.order() != ByteOrder.LITTLE_ENDIAN) {
            throw new IllegalStateException("Expected ticket to be in LITTLE_ENDIAN order");
        }
        int pos = ticket.position();
        if (ticket.remaining() != 5 || ticket.get(pos) != TICKET_PREFIX) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Cannot parse ticket: found 0x" + byteBufToHex(ticket) + " (hex)");
        }
        return ticket.getInt(pos + 1);
    }
}
