package io.deephaven.client.impl;

import dagger.Binds;
import dagger.Module;

@Module
public interface FlightModule {

    @Binds
    Flight providesFlight(FlightImpl impl);
}
