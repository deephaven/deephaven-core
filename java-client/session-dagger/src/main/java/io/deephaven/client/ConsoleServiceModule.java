package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import dagger.Reusable;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceBlockingStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceFutureStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.grpc.Channel;

@Module
public interface ConsoleServiceModule {

    @Provides
    @Reusable
    static ConsoleServiceStub providesStub(Channel channel) {
        return ConsoleServiceGrpc.newStub(channel);
    }

    @Provides
    @Reusable
    static ConsoleServiceBlockingStub providesBlockingStub(Channel channel) {
        return ConsoleServiceGrpc.newBlockingStub(channel);
    }

    @Provides
    @Reusable
    static ConsoleServiceFutureStub providesFutureStub(Channel channel) {
        return ConsoleServiceGrpc.newFutureStub(channel);
    }
}
