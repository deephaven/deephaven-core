package io.deephaven.grpc_api.util;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteOrder;

public class FlightExportTicketHelper {
    /**
     * Convenience method to convert from export id to {@link Flight.Ticket}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Flight.Ticket exportIdToFlightTicket(int exportId) {
        final byte[] dest = ExportTicketHelper.exportIdToBytes(exportId);
        return Flight.Ticket.newBuilder().setTicket(ByteStringAccess.wrap(dest)).build();
    }

    /**
     * Convenience method to convert from export id to {@link Flight.FlightDescriptor}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Flight.FlightDescriptor exportIdToDescriptor(int exportId) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addPath(ExportTicketHelper.FLIGHT_DESCRIPTOR_ROUTE)
                .addPath(Integer.toString(exportId)).build();
    }

    /**
     * Convenience method to convert from {@link Flight.Ticket} to export id.
     *
     * <p>
     * Ticket's byte[0] must be {@link ExportTicketHelper#TICKET_PREFIX}, bytes[1-4] are a signed int export id in
     * little-endian.
     *
     * @param ticket the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the export id that the Ticket wraps
     */
    public static int ticketToExportId(final Flight.Ticket ticket, final String logId) {
        return ExportTicketHelper.ticketToExportIdInternal(
                ticket.getTicket().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), logId);
    }

    /**
     * Convenience method to convert from {@link Flight.FlightDescriptor} to export id.
     *
     * <p>
     * Descriptor must be a path.
     *
     * @param descriptor the grpc Ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the export id that the Ticket wraps
     */
    public static int descriptorToExportId(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': is empty");
        }
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': not a path");
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }

        try {
            return Integer.parseInt(descriptor.getPath(1));
        } catch (final NumberFormatException nfe) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': export id not numeric (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ")");
        }
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Ticket ticket, final String logId) {
        return exportIdToDescriptor(ExportTicketHelper.ticketToExportId(ticket, logId));
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket, final String logId) {
        return exportIdToDescriptor(ticketToExportId(ticket, logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToFlightTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return exportIdToFlightTicket(descriptorToExportId(descriptor, logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return ExportTicketHelper.wrapExportIdInTicket(descriptorToExportId(descriptor, logId));
    }
}
