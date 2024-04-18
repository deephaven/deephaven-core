//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import dagger.Component;
import io.deephaven.client.impl.BarrageSubcomponent.Builder;
import io.deephaven.client.impl.BarrageSubcomponent.DeephavenClientSubcomponentModule;

/**
 * Component for creating {@link BarrageSubcomponent}.
 *
 * @see DeephavenClientSubcomponentModule
 */
@Component(modules = DeephavenClientSubcomponentModule.class)
public interface DeephavenBarrageRoot {

    /**
     * Equivalent to {@code DaggerDeephavenBarrageRoot.create()}.
     *
     * @return the barrage root
     */
    static DeephavenBarrageRoot of() {
        return DaggerDeephavenBarrageRoot.create();
    }

    Builder factoryBuilder();
}
