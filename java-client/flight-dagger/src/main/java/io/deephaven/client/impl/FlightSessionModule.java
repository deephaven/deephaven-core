//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Provides {@link FlightSession}.
 */
@Module
public class FlightSessionModule {

    /**
     * Delegates to {@link FlightSession#of(SessionImpl, BufferAllocator, ManagedChannel)}.
     */
    @Provides
    public static FlightSession newFlightSession(SessionImpl session, BufferAllocator allocator,
            ManagedChannel managedChannel) {
        return FlightSession.of(session, allocator, managedChannel);
    }
}
