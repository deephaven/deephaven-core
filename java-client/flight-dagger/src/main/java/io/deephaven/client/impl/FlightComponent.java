package io.deephaven.client.impl;

import dagger.BindsInstance;
import dagger.Component;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.concurrent.ScheduledExecutorService;

@Component(modules = {FlightModule.class, FlightClientModule.class})
public interface FlightComponent {

    SessionAndFlight sessionAndFlight();

    @Component.Factory
    interface Factory {
        FlightComponent create(FlightClientModule flightClientModule,
            @BindsInstance ManagedChannel managedChannel,
            @BindsInstance ScheduledExecutorService scheduler,
            @BindsInstance BufferAllocator bufferAllocator);
    }
}
