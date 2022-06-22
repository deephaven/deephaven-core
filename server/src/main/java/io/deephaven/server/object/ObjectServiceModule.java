/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.object;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface ObjectServiceModule {
    @Binds
    @IntoSet
    BindableService bindObjectServiceGrpcImpl(ObjectServiceGrpcImpl objectService);
}
