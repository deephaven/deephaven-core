/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client;

import dagger.Component;
import io.deephaven.client.SessionSubcomponent.Builder;
import io.deephaven.client.SessionSubcomponent.SessionFactorySubcomponentModule;

@Component(modules = SessionFactorySubcomponentModule.class)
public interface DeephavenSessionRoot {

    Builder factoryBuilder();
}
