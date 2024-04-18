//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;

/**
 * Module that provides {@link BarrageSessionFactoryBuilder}.
 */
@Module
public interface BarrageFactoryBuilderModule {

    /**
     * Equivalent to {@code DeephavenBarrageRoot.of().factoryBuilder()}.
     *
     * @return the barrage session factory builder
     * @see DeephavenBarrageRoot
     */
    @Provides
    static BarrageSessionFactoryBuilder providesFactoryBuilder() {
        return DeephavenBarrageRoot.of().factoryBuilder();
    }
}
