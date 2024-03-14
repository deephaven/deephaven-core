//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;

public interface BarrageSessionFactoryBuilder {
    BarrageSessionFactoryBuilder managedChannel(ManagedChannel channel);

    BarrageSessionFactoryBuilder scheduler(ScheduledExecutorService scheduler);

    BarrageSessionFactoryBuilder allocator(BufferAllocator bufferAllocator);

    BarrageSessionFactoryBuilder authenticationTypeAndValue(@Nullable String authenticationTypeAndValue);

    BarrageSessionFactory build();
}
