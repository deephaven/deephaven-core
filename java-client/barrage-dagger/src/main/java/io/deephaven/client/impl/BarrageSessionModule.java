//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Provides {@link BarrageSession}.
 */
@Module
public class BarrageSessionModule {

    /**
     * Delegates to {@link BarrageSession#of(SessionImpl, BufferAllocator, ManagedChannel)}.
     */
    @Provides
    public static BarrageSession newDeephavenClientSession(
            SessionImpl session, BufferAllocator allocator, ManagedChannel managedChannel) {
        return BarrageSession.of(session, allocator, managedChannel);
    }
}
