//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;

@Module
public interface ObjectServiceModule {
    @Binds
    @IntoSet
    BindableService bindObjectServiceGrpcImpl(ObjectServiceGrpcBinding objectService);

    /**
     * Provides the {@link PluginReferenceAuthorizationPolicy} from the configuration property
     * {@code PluginReferenceAuthorization.failClosed} (default {@code false}, i.e.
     * {@link PluginReferenceAuthorizationPolicy#PERMISSIVE}).
     */
    @Provides
    static PluginReferenceAuthorizationPolicy providesReferenceAuthorizationPolicy() {
        return Configuration.getInstance().getBooleanWithDefault("PluginReferenceAuthorization.failClosed", false)
                ? PluginReferenceAuthorizationPolicy.FAIL_CLOSED
                : PluginReferenceAuthorizationPolicy.PERMISSIVE;
    }
}
