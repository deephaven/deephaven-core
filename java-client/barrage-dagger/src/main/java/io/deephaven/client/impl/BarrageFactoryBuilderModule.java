/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.Module;
import dagger.Provides;

@Module
public interface BarrageFactoryBuilderModule {

    @Provides
    static BarrageSessionFactoryBuilder providesFactoryBuilder() {
        return DaggerDeephavenBarrageRoot.create().factoryBuilder();
    }
}
