package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Module
public class FlightSessionModule {

    @Provides
    public static FlightSession newFlightSession(SessionImpl session, BufferAllocator allocator,
            ManagedChannel managedChannel) {
        return FlightSession.of(session, allocator, managedChannel);
    }

    @Provides
    public static CompletableFuture<? extends FlightSession> newFlightSessionFuture(
            CompletableFuture<? extends SessionImpl> sessionFuture, BufferAllocator allocator,
            ManagedChannel managedChannel) {
        return sessionFuture
                .thenApply((Function<SessionImpl, FlightSession>) session -> FlightSession.of(session,
                        allocator, managedChannel));
    }
}
