package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import dagger.Reusable;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceFutureStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.grpc.Channel;

@Module
public interface SessionServiceModule {

    @Provides
    @Reusable
    static SessionServiceStub providesStub(Channel channel) {
        return SessionServiceGrpc.newStub(channel);
    }

    @Provides
    @Reusable
    static SessionServiceBlockingStub providesBlockingStub(Channel channel) {
        return SessionServiceGrpc.newBlockingStub(channel);
    }

    @Provides
    @Reusable
    static SessionServiceFutureStub providesFutureStub(Channel channel) {
        return SessionServiceGrpc.newFutureStub(channel);
    }
}
