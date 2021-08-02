package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import dagger.Reusable;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.grpc.Channel;

@Module
public interface TableServiceModule {

    @Provides
    @Reusable
    static TableServiceStub providesStub(Channel channel) {
        return TableServiceGrpc.newStub(channel);
    }

    @Provides
    @Reusable
    static TableServiceBlockingStub providesBlockingStub(Channel channel) {
        return TableServiceGrpc.newBlockingStub(channel);
    }

    @Provides
    @Reusable
    static TableServiceFutureStub providesFutureStub(Channel channel) {
        return TableServiceGrpc.newFutureStub(channel);
    }
}
