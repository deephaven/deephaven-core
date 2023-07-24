package io.deephaven.server.grpc;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import java.util.Collections;
import java.util.Set;

@Module
public class GrpcModule {
    @Provides
    @ElementsIntoSet
    static Set<BindableService> primeServices() {
        return Collections.emptySet();
    }

    @Provides
    @ElementsIntoSet
    static Set<ServerInterceptor> primeInterceptors() {
        return Collections.emptySet();
    }
}
