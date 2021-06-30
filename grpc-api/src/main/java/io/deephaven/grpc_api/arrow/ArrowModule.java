package io.deephaven.grpc_api.arrow;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface ArrowModule {
    @Binds @IntoSet
    BindableService bindFlightServiceBinding(FlightServiceGrpcBinding service);
}
