//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.server.session.TicketResolver;

import java.util.Collections;
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
    static Set<TicketResolver> provideTicketResolver() {
        return ServiceLoader.load(TicketResolversFromServiceLoader.Factory.class)
                .stream()
                .map(factory -> factory.get().create())
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    /**
     * Creates a TicketResolver for this process.
     */
    public interface Factory {
        /**
         * @return a TicketResolver to bind into this process.
         */
        TicketResolver create();
    }
}
