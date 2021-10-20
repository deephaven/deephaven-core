/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.uri.TableResolver;

@Module
public interface BarrageUriModule {

    @Binds
    @IntoSet
    TableResolver bindsBarrageTableResolver(BarrageTableResolver resolver);
}
