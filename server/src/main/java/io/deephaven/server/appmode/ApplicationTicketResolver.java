package io.deephaven.server.appmode;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.flight.util.TicketRouterHelper;
import io.deephaven.proto.util.ApplicationTicketHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Singleton
public class ApplicationTicketResolver extends TicketResolverBase implements ApplicationStates {

    private final Map<String, ApplicationState> applicationMap = new ConcurrentHashMap<>();

    @Inject
    public ApplicationTicketResolver() {
        super((byte) ApplicationTicketHelper.TICKET_PREFIX, ApplicationTicketHelper.FLIGHT_DESCRIPTOR_ROUTE);
    }

    @Override
    public final Optional<ApplicationState> getApplicationState(String applicationId) {
        return Optional.ofNullable(applicationMap.get(applicationId));
    }

    public synchronized void onApplicationLoad(final ApplicationState app) {
        if (applicationMap.containsKey(app.id())) {
            if (applicationMap.get(app.id()) != app) {
                throw new IllegalArgumentException("Duplicate application found for app_id " + app.id());
            }
            return;
        }

        applicationMap.put(app.id(), app);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            final @Nullable SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(appFieldIdFor(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            final @Nullable SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(appFieldIdFor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(final AppFieldId id, final String logId) {
        if (id.app == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': field '" + getLogNameFor(id)
                            + "' does not belong to an application");
        }
        final Field<Object> field = id.app.getField(id.fieldName);
        if (field == null) {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': field '" + getLogNameFor(id) + "' not found");
        }
        // noinspection unchecked
        return SessionState.wrapAsExport((T) field.value());
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            final @Nullable SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        final AppFieldId id = appFieldIdFor(descriptor, logId);
        if (id.app == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': field does not belong to an application");
        }

        final Flight.FlightInfo info;
        synchronized (id.app) {
            Field<?> field = id.app.getField(id.fieldName);
            if (field == null) {
                throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                        "Could not resolve '" + logId + "': field '" + getLogNameFor(id) + "' not found");
            }
            Object value = field.value();
            if (value instanceof Table) {
                info = TicketRouter.getFlightInfo((Table) value, descriptor, flightTicketForName(id.app, id.fieldName));
            } else {
                throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                        "Could not resolve '" + logId + "': field '" + getLogNameFor(id) + "' is not a flight");
            }
        }

        return SessionState.wrapAsExport(info);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            SessionState session, ByteBuffer ticket, final String logId) {
        throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': application tickets cannot be published to");
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': application flight descriptors cannot be published to");
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return getLogNameFor(appFieldIdFor(ticket, logId));
    }

    private String getLogNameFor(final AppFieldId id) {
        return "app/" + id.applicationId() + "/field/" + id.fieldName;
    }

    @Override
    public void forAllFlightInfo(@Nullable SessionState session, Consumer<Flight.FlightInfo> visitor) {
        applicationMap.values().forEach(app -> {
            app.listFields().forEach(field -> {
                Object value = field.value();
                if (value instanceof Table) {
                    final Flight.FlightInfo info = TicketRouter.getFlightInfo((Table) value,
                            descriptorForName(app, field.name()), flightTicketForName(app, field.name()));
                    visitor.accept(info);
                }
            });
        });
    }

    /**
     * Convenience method to convert from an application variable name to Ticket
     *
     * @param app the application state that this field is defined in
     * @param name the application variable name to convert
     * @return the ticket this descriptor represents
     */
    public static Ticket ticketForName(final ApplicationState app, final String name) {
        final byte[] ticket = ApplicationTicketHelper.applicationFieldToBytes(app.id(), name);
        return Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from an application variable name to Flight.Ticket
     *
     * @param app the application state that this field is defined in
     * @param name the application variable name to convert
     * @return the ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForName(final ApplicationState app, final String name) {
        final byte[] ticket = ApplicationTicketHelper.applicationFieldToBytes(app.id(), name);
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to Flight.FlightDescriptor
     *
     * @param app the application state that this field is defined in
     * @param name the application variable name to convert
     * @return the flight descriptor this descriptor represents
     */
    public static Flight.FlightDescriptor descriptorForName(final ApplicationState app, final String name) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addAllPath(ApplicationTicketHelper.applicationFieldToPath(app.id(), name))
                .build();
    }

    private AppFieldId appFieldIdFor(final ByteBuffer ticket, final String logId) {
        if (ticket == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': ticket not supplied");
        }

        final String ticketAsString;
        final int initialLimit = ticket.limit();
        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        try {
            ticketAsString = decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
            ticket.limit(initialLimit);
        }

        final int endOfRoute = ticketAsString.indexOf('/');
        final int endOfAppId = ticketAsString.indexOf('/', endOfRoute + 1);
        final int endOfFieldSegment = ticketAsString.indexOf('/', endOfAppId + 1);
        if (endOfAppId == -1 || endOfFieldSegment == -1) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': ticket does conform to expected format");
        }
        final String appId = ticketAsString.substring(endOfRoute + 1, endOfAppId);
        final String fieldName = ticketAsString.substring(endOfFieldSegment + 1);

        final ApplicationState app = applicationMap.get(appId);
        if (app == null) {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': no application exists with the identifier: " + appId);
        }

        return AppFieldId.from(app, fieldName);
    }

    private AppFieldId appFieldIdFor(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': descriptor not supplied");
        }

        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': only flight paths are supported");
        }

        // current structure: a/app_id/f/field_name
        if (descriptor.getPathCount() != 4) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 4)");
        }

        final String appId = descriptor.getPath(1);
        final ApplicationState app = applicationMap.get(appId);
        if (app == null) {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': no application exists with the identifier: " + appId);
        }

        if (!descriptor.getPath(2).equals(ApplicationTicketHelper.FIELD_PATH_SEGMENT)) {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': path is not an application field");
        }

        return AppFieldId.from(app, descriptor.getPath(3));
    }
}
