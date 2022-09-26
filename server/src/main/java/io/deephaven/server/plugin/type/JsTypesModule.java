/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.plugin.Registration;

/**
 * Binds {@link JsTypeDistributionFromPackageJsonSystemPropertyRegistration} into the set of {@link Registration}.
 */
@Module
public interface JsTypesModule {

    @Binds
    @IntoSet
    Registration bindsPackageJsonSystemPropertyRegistration(
            JsTypeDistributionFromPackageJsonSystemPropertyRegistration registration);
}
