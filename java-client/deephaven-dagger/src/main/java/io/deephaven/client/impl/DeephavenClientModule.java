/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Module
public class DeephavenClientModule {
    @Provides
    public static DeephavenClientSession newDeephavenClientSession(
            SessionImpl session, BufferAllocator allocator, ManagedChannel managedChannel) {
        return DeephavenClientSession.of(session, allocator, managedChannel);
    }

    @Provides
    public static CompletableFuture<? extends DeephavenClientSession> newDeephavenClientSessionFuture(
            CompletableFuture<? extends SessionImpl> sessionFuture, BufferAllocator allocator,
            ManagedChannel managedChannel) {
        return sessionFuture.thenApply((Function<SessionImpl, DeephavenClientSession>) session -> DeephavenClientSession
                .of(session, allocator, managedChannel));
    }
}
