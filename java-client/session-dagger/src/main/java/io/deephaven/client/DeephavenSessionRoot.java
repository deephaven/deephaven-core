//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import dagger.Component;
import io.deephaven.client.SessionSubcomponent.Builder;
import io.deephaven.client.SessionSubcomponent.SessionFactorySubcomponentModule;

/**
 * Component for creating {@link SessionSubcomponent}.
 *
 * @see SessionFactorySubcomponentModule
 */
@Component(modules = SessionFactorySubcomponentModule.class)
public interface DeephavenSessionRoot {

    /**
     * Equivalent to {@code DaggerDeephavenSessionRoot.create()}.
     *
     * @return the session root
     */
    static DeephavenSessionRoot of() {
        return DaggerDeephavenSessionRoot.create();
    }

    Builder factoryBuilder();
}
