//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin;

import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.options.PluginOptions;
import io.deephaven.server.auth.AuthorizationProvider;

import javax.inject.Named;
import java.util.function.Predicate;

/**
 * Adapts an {@link io.deephaven.server.auth.AuthorizationProvider} to functions for use in PluginOptions.
 */
@Module()
public class AuthorizationProviderAdapterModule {
    @Provides
    @Named("authTransform")
    PluginOptions.AuthorizationTransformer authTransform(final AuthorizationProvider provider) {
        return new PluginOptions.AuthorizationTransformer() {
            @Override
            public <T> T transform(T object) {
                return provider.getTicketResolverAuthorization().transform(object);
            }
        };
    }

    @Provides
    @Named("accessPermittedPredicate")
    Predicate<Object> accessPermitted(final AuthorizationProvider provider) {
        return x -> !provider.getTicketResolverAuthorization().isDeniedAccess(x);
    }
}
