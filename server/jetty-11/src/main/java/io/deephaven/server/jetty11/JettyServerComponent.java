//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import dagger.BindsInstance;
import io.deephaven.server.runner.DeephavenApiServerComponent;

public interface JettyServerComponent extends DeephavenApiServerComponent {

    interface Builder<Self extends Builder<Self, Component>, Component extends JettyServerComponent>
            extends DeephavenApiServerComponent.Builder<Self, Component> {

        @BindsInstance
        Self withJettyConfig(JettyConfig config);
    }
}
