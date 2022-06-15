/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.auth;

import dagger.Binds;
import dagger.Module;

import javax.inject.Singleton;

@Module()
public interface AuthContextModule {
    @Binds
    @Singleton
    AuthContextProvider bindAuthContextProvider(TrivialAuthContextProvider authContextProvider);
}
