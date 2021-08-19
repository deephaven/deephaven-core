package io.deephaven.grpc_api.browserstreaming;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.ServerInterceptor;

@Module
public interface BrowserStreamModule {
    @Binds
    @IntoSet
    ServerInterceptor bindSessionServiceInterceptor(BrowserStreamInterceptor browserStreamInterceptor);
}
