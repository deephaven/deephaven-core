/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.jetty.JettyMain.JettyMainComponent;
import io.deephaven.server.jetty.JettyMain.JettyMainComponent.Builder;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.MainBase;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public final class JettyMain extends MainBase<JettyMain, Builder, JettyMainComponent> {

    @Singleton
    @Component(modules = {
            DefaultsModule.class,
            JettyServerModule.class,
            CommunityAuthorizationModule.class,
    })
    public interface JettyMainComponent extends DeephavenApiServerComponent {
        @Component.Builder
        interface Builder extends DeephavenApiServerComponent.Builder<Builder, JettyMainComponent> {
            @BindsInstance
            Builder withJettyConfig(JettyConfig config);
        }
    }

    @Override
    public JettyMainComponent.Builder builderFrom(Configuration config) {
        final JettyConfig jettyConfig = JettyConfig.buildFromConfig(config).build();
        return DaggerJettyMain_JettyMainComponent
                .builder()
                .withJettyConfig(jettyConfig);
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        new JettyMain().main(args, JettyMain.class);
    }
}
