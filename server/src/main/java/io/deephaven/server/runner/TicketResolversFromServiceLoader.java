//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.TicketResolver;
import org.immutables.value.Value;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Loads a set of TicketResolvers that implement the Factory class.
 */
@Module
public class TicketResolversFromServiceLoader {
    @Provides
    @ElementsIntoSet
    static Set<TicketResolver> provideTicketResolver(AuthorizationProvider authorizationProvider) {
        final TicketResolverOptions options =
                TicketResolverOptions.builder().authorizationProvider(authorizationProvider).build();
        return ServiceLoader.load(TicketResolversFromServiceLoader.Factory.class)
                .stream()
                .map(factory -> factory.get().create(options))
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Creates a TicketResolver for this process.
     */
    public interface Factory {
        /**
         * @return a TicketResolver to bind into this process.
         */
        TicketResolver create(final TicketResolverOptions options);
    }

    /**
     * A set of injected values for use by the service loaded ticket resolvers.
     */
    @Value.Immutable
    @BuildableStyle
    public abstract static class TicketResolverOptions {
        static ImmutableTicketResolverOptions.Builder builder() {
            return ImmutableTicketResolverOptions.builder();
        }

        /**
         * @return the AuthorizationProvider for these ticket resolvers.
         */
        public abstract AuthorizationProvider authorizationProvider();
    }
}
