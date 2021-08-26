package io.deephaven.client;

import dagger.Component;
import io.deephaven.client.SessionSubcomponent.Builder;
import io.deephaven.client.SessionSubcomponent.SessionFactorySubcomponentModule;

@Component(modules = SessionFactorySubcomponentModule.class)
public interface DeephavenSessionRoot {

    Builder factoryBuilder();
}
