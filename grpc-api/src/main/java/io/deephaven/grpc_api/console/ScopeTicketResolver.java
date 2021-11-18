/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.console;

import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketResolverBase;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.util.FlightScopeTicketHelper;
import io.deephaven.grpc_api.util.ScopeTicketHelper;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightInfo;
import org.apache.arrow.flight.impl.Flight.Ticket;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

@Singleton
public class ScopeTicketResolver extends TicketResolverBase {

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public ScopeTicketResolver(final GlobalSessionProvider globalSessionProvider) {
        super((byte) ScopeTicketHelper.TICKET_PREFIX, ScopeTicketHelper.FLIGHT_DESCRIPTOR_ROUTE);
        this.globalSessionProvider = globalSessionProvider;
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return ScopeTicketHelper.FLIGHT_DESCRIPTOR_ROUTE + "/" + FlightScopeTicketHelper.nameForTicket(ticket, logId);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        // there is no mechanism to wait for a scope variable to resolve; require that the scope variable exists now
        final String scopeName = FlightScopeTicketHelper.nameForDescriptor(descriptor, logId);

        final Flight.FlightInfo flightInfo = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> {
            final ScriptSession gss = globalSessionProvider.getGlobalSession();
            Object scopeVar = gss.getVariable(scopeName, null);
            if (scopeVar == null) {
                throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                        "Could not resolve '" + logId + ": no variable exists with name '" + scopeName + "'");
            }
            if (scopeVar instanceof Table) {
                return TicketRouter.getFlightInfo((Table) scopeVar, descriptor,
                        FlightScopeTicketHelper.flightTicketForName(scopeName));
            }

            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': no variable exists with name '" + scopeName + "'");
        });

        return SessionState.wrapAsExport(flightInfo);
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        globalSessionProvider.getGlobalSession().getVariables().forEach((varName, varObj) -> {
            if (varObj instanceof Table) {
                final FlightDescriptor descriptor = FlightScopeTicketHelper.flightDescriptorForName(varName);
                final Ticket ticket = FlightScopeTicketHelper.flightTicketForName(varName);
                final FlightInfo flightInfo = TicketRouter.getFlightInfo((Table) varObj, descriptor, ticket);
                visitor.accept(flightInfo);
            }
        });
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(session, FlightScopeTicketHelper.nameForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(session, FlightScopeTicketHelper.nameForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final String scopeName, final String logId) {
        // if we are not attached to a session, check the scope for a variable right now
        final T export = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> {
            final ScriptSession gss = globalSessionProvider.getGlobalSession();
            // noinspection unchecked
            T scopeVar = (T) gss.unwrapObject(gss.getVariable(scopeName));
            if (scopeVar == null) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Could not resolve '" + logId + "': no variable exists with name '" + scopeName + "'");
            }
            return scopeVar;
        });

        if (export == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no variable exists with name '" + scopeName + "'");
        }
        return SessionState.wrapAsExport(export);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final ByteBuffer ticket, final String logId) {
        return publish(session, FlightScopeTicketHelper.nameForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return publish(session, FlightScopeTicketHelper.nameForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final String varName, final String logId) {
        // We publish to the query scope after the client finishes publishing their result. We accomplish this by
        // directly depending on the result of this export builder.
        final SessionState.ExportBuilder<T> resultBuilder = session.nonExport();
        final SessionState.ExportObject<T> resultExport = resultBuilder.getExport();
        final SessionState.ExportBuilder<T> publishTask = session.nonExport();

        publishTask
                .requiresSerialQueue()
                .require(resultExport)
                .submit(() -> {
                    final ScriptSession gss = globalSessionProvider.getGlobalSession();
                    gss.setVariable(varName, resultExport.get());
                });

        return resultBuilder;
    }
}
