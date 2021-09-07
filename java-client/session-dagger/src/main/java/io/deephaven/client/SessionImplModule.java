package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.grpc_api.DeephavenChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface SessionImplModule {

    @Provides
    static SessionImpl session(DeephavenChannel channel, ScheduledExecutorService scheduler) {
        return SessionImplConfig.builder()
                .executor(scheduler)
                .channel(channel)
                .build()
                .createSession();
    }

    @Provides
    static CompletableFuture<? extends SessionImpl> sessionFuture(DeephavenChannel channel,
            ScheduledExecutorService scheduler) {
        return SessionImplConfig.builder()
                .executor(scheduler)
                .channel(channel)
                .build()
                .createSessionFuture();
    }
}
