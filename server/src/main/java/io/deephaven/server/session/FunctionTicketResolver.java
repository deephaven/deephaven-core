//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.flight.util.TicketRouterHelper;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.FunctionTicketHelper;
import io.deephaven.server.auth.AuthorizationProvider;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.deephaven.proto.util.FunctionTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.proto.util.FunctionTicketHelper.TICKET_PREFIX;

/**
 * A resolver for handling function tickets in the Deephaven environment.
 *
 * <p>
 * Function tickets have a prefix of 'f/' and are a fixed set of functions exposed by the Deephaven server. The primary
 * use case is for bidirectional plugins to register themselves on startup outside at a well-known name for plugin
 * clients to use without needing to inject additional variables into the session scope.
 * </p>
 */
@Singleton
public class FunctionTicketResolver extends TicketResolverBase {

    private final Map<String, Object> functionMap = new ConcurrentHashMap<>();

    /**
     * Supplies a Map of function names to objects to be registered on resolver creation.
     *
     * <p>
     * If conflicting function names are registered, then an {@link IllegalStateException} is thrown.
     * </p>
     *
     * <p>
     * Note service loading this interface is <b>experimental</b> and subject to change in future versions.
     * </p>
     */
    public interface FunctionSupplier {
        Map<String, ?> getFunctions();
    }

    @Inject
    public FunctionTicketResolver(
            final AuthorizationProvider authProvider,
            final Set<FunctionSupplier> functionSuppliers) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        functionSuppliers.forEach(fs -> {
            final Map<String, ?> newFunctions = fs.getFunctions();
            for (final Map.Entry<String, ?> entry : newFunctions.entrySet()) {
                register(entry.getKey(), entry.getValue());
            }
        });
    }

    /**
     * Register a function with the given name
     * 
     * @param name the name of the function to register
     * @param function the object to associate with the name
     * @throws IllegalStateException if conflicting function names are registered
     */
    public void register(final String name, final Object function) {
        final Object old = functionMap.putIfAbsent(name, function);
        if (old != null) {
            throw new IllegalStateException("Cannot register duplicate function name '" + name + "'");
        }
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return String.format("%s/%s", FLIGHT_DESCRIPTOR_ROUTE, nameForTicket(ticket, logId));
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        // These tickets are not intended to point at a table, so they are always not found for flight info.
        throw newNotFoundSRE(logId, nameForDescriptor(descriptor, logId));
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // These tickets are not intended to point at a table, therefore we don't need to add anything to flight info
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(session, nameForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(session, nameForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final String name, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, String.format(
                    "Could not resolve '%s': no session to handoff to", logId));
        }

        final Object function = functionMap.get(name);
        if (function == null) {
            return SessionState.wrapAsFailedExport(newNotFoundSRE(logId, name));
        }

        // noinspection unchecked
        final SessionState.ExportObject<T> export = SessionState.wrapAsExport((T) function);

        // we need to wrap this in a new export object to hand off to the new session and defer checking permissions
        return session.<T>nonExport()
                .require(export)
                .submit(() -> {
                    T result = export.get();
                    result = authorization.transform(result);
                    if (result == null) {
                        throw newNotFoundSRE(logId, name);
                    }
                    return result;
                });
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': function tickets cannot be published to");
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': function tickets cannot be published to");
    }

    @Override
    public <T> void publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish,
            final SessionState.ExportErrorHandler errorHandler,
            final SessionState.ExportObject<T> source) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': function tickets cannot be published to");
    }

    /**
     * Convenience method to convert from a function name to Flight.Ticket
     *
     * @param name the function name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForName(final String name) {
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(FunctionTicketHelper.nameToBytes(name)))
                .build();
    }

    /**
     * Convenience method to convert from a function name to Ticket
     *
     * @param name the function name to convert
     * @return the flight ticket this descriptor represents
     */
    public static io.deephaven.proto.backplane.grpc.Ticket ticketForName(final String name) {
        return Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(FunctionTicketHelper.nameToBytes(name)))
                .build();
    }

    /**
     * Convenience method to convert from a function name to Flight.FlightDescriptor
     *
     * @param name the function name to convert
     * @return the flight descriptor this descriptor represents
     */
    public static Flight.FlightDescriptor descriptorForName(final String name) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addAllPath(FunctionTicketHelper.nameToPath(name))
                .build();
    }

    /**
     * Convenience method to convert from a Flight.Ticket (as ByteBuffer) to function name
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the function name this ticket represents
     */
    public static String nameForTicket(final ByteBuffer ticket, final String logId) {
        if (ticket == null || ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no ticket supplied");
        }
        if (ticket.remaining() < 3 || ticket.get(ticket.position()) != FunctionTicketHelper.TICKET_PREFIX
                || ticket.get(ticket.position() + 1) != '/') {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': found 0x" + ByteHelper.byteBufToHex(ticket) + "' (hex)");
        }

        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        try {
            ticket.position(initialPosition + 2);
            return decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
        }
    }

    /**
     * Convenience method to convert from a Flight.FlightDescriptor to function Name
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the function name this ticket represents
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
        if (!descriptor.getPath(0).equals(FunctionTicketHelper.FLIGHT_DESCRIPTOR_ROUTE)) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: "
                            + FunctionTicketHelper.FLIGHT_DESCRIPTOR_ROUTE
                            + ")");
        }

        return descriptor.getPath(1);
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket, final String logId) {
        return descriptorForName(nameForTicket(ticket.getTicket().asReadOnlyByteBuffer(), logId));
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

    private static @NotNull StatusRuntimeException newNotFoundSRE(final String logId, final String sharedId) {
        return Exceptions.statusRuntimeException(Code.NOT_FOUND, String.format(
                "Could not resolve '%s': ticket '%s' not found", logId, sharedId));
    }
}
