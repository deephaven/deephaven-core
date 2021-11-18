package io.deephaven.grpc_api.util;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public class FlightScopeTicketHelper {

    /**
     * Convenience method to convert from a scoped variable name to Flight.Ticket
     *
     * @param name the scoped variable name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForName(final String name) {
        final byte[] ticket = ScopeTicketHelper.nameToBytes(name);
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to Ticket
     *
     * @param name the scoped variable name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Ticket ticketForName(final String name) {
        final byte[] ticket = ScopeTicketHelper.nameToBytes(name);
        return Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to {@link Flight.FlightDescriptor}.
     *
     * @param name the scoped variable name to convert
     * @return the flight descriptor this name represents
     */
    public static Flight.FlightDescriptor flightDescriptorForName(final String name) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addAllPath(ScopeTicketHelper.nameToPath(name))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to {@link FlightDescriptor}.
     *
     * @param name the scoped variable name to convert
     * @return the flight descriptor this name represents
     */
    public static FlightDescriptor descriptorForName(final String name) {
        return ArrowHelper.descriptor(flightDescriptorForName(name));
    }

    /**
     * Convenience method to convert from a Flight.Ticket (as ByteBuffer) to scope variable name
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this ticket represents
     */
    public static String nameForTicket(final ByteBuffer ticket, final String logId) {
        if (ticket == null || ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no ticket supplied");
        }
        if (ticket.remaining() < 3 || ticket.get(ticket.position()) != ScopeTicketHelper.TICKET_PREFIX
                || ticket.get(ticket.position() + 1) != '/') {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': found 0x" + ByteHelper.byteBufToHex(ticket) + "' (hex)");
        }

        final int initialLimit = ticket.limit();
        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        try {
            ticket.position(initialPosition + 2);
            return decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
            ticket.limit(initialLimit);
        }
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket, final String logId) {
        return flightDescriptorForName(nameForTicket(ticket.getTicket().asReadOnlyByteBuffer(), logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return flightTicketForName(nameForDescriptor(descriptor, logId));
    }

    /**
     * Convenience method to convert from a Flight.FlightDescriptor to scoped Variable Name
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this descriptor represents
     */
    public static String nameForDescriptor(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': only paths are supported");
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }

        return descriptor.getPath(1);
    }
}
