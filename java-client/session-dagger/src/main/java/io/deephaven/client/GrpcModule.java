package io.deephaven.client;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

@Module
public interface GrpcModule {

    @Binds
    Channel bindsChannel(ManagedChannel managedChannel);
}
