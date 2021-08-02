package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;
import dagger.Reusable;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Collections;
import java.util.Objects;

@Module
public class FlightClientModule {

    private final SessionImpl session;

    public FlightClientModule(SessionImpl session) {
        this.session = Objects.requireNonNull(session);
    }

    @Provides
    @Reusable
    SessionMiddleware providesSessionMiddleware() {
        return new SessionMiddleware(session);
    }

    @Provides
    FlightClient providesFlightClient(BufferAllocator incomingAllocator, ManagedChannel channel,
        SessionMiddleware sessionMiddleware) {
        return FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(incomingAllocator,
            channel, Collections.singletonList(sessionMiddleware));
    }

    @Provides
    SessionAndFlight providesSessionAndFlight(Flight flight) {
        return new SessionAndFlight() {
            @Override
            public Session session() {
                return session;
            }

            @Override
            public Flight flight() {
                return flight;
            }
        };
    }
}
