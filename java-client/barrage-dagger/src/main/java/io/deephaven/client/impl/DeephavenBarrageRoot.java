/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.Component;
import io.deephaven.client.impl.BarrageSubcomponent.Builder;
import io.deephaven.client.impl.BarrageSubcomponent.DeephavenClientSubcomponentModule;

@Component(modules = DeephavenClientSubcomponentModule.class)
public interface DeephavenBarrageRoot {

    Builder factoryBuilder();
}
