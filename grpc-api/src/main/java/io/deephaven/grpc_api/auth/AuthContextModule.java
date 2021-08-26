package io.deephaven.grpc_api.auth;

import dagger.Binds;
import dagger.Module;

import javax.inject.Singleton;

@Module()
public interface AuthContextModule {
    @Binds
    @Singleton
    AuthContextProvider bindAuthContextProvider(TrivialAuthContextProvider authContextProvider);
}
