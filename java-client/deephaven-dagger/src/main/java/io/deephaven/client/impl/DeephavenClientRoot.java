/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.Component;
import io.deephaven.client.impl.DeephavenClientSubcomponent.Builder;
import io.deephaven.client.impl.DeephavenClientSubcomponent.DeephavenClientSubcomponentModule;

@Component(modules = DeephavenClientSubcomponentModule.class)
public interface DeephavenClientRoot {

    Builder factoryBuilder();
}
