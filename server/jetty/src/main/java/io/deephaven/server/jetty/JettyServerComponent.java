/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import dagger.BindsInstance;
import io.deephaven.server.runner.DeephavenApiServerComponent;

public interface JettyServerComponent extends DeephavenApiServerComponent {

    interface Builder<Self extends Builder<Self, Component>, Component extends JettyServerComponent>
            extends DeephavenApiServerComponent.Builder<Self, Component> {

        @BindsInstance
        Self withJettyConfig(JettyConfig config);
    }
}
