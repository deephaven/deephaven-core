//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.DeephavenChannelImpl;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface SessionImplModule {

    @Binds
    Channel bindsManagedChannel(ManagedChannel managedChannel);

    @Binds
    DeephavenChannel bindsDeephavenChannelImpl(DeephavenChannelImpl deephavenChannelImpl);

    @Provides
    static SessionImplConfig providesSessionImplConfig(
            DeephavenChannel channel,
            ScheduledExecutorService scheduler,
            @Nullable @Named("authenticationTypeAndValue") String authenticationTypeAndValue) {
        return SessionImplConfig.of(channel, scheduler, authenticationTypeAndValue);
    }

    @Provides
    static SessionImpl session(SessionImplConfig config) {
        try {
            return config.createSession();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
