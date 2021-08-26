package io.deephaven.client.impl;

import dagger.Component;
import io.deephaven.client.impl.FlightSubcomponent.Builder;
import io.deephaven.client.impl.FlightSubcomponent.FlightSubcomponentModule;

@Component(modules = FlightSubcomponentModule.class)
public interface DeephavenFlightRoot {

    Builder factoryBuilder();
}
