/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.custom;

import dagger.Binds;
import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.jetty.JettyServerComponent;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.jetty.JettyServerModule;
import io.deephaven.server.runner.CommunityDefaultsModule;
import io.deephaven.server.runner.ComponentBuilderBase;

import javax.inject.Singleton;
import java.io.PrintStream;

/**
 * An example of a "custom integrator" component builder. This is not meant to be an exhaustive example of Deephaven
 * configuration points, nor dagger conventions, but rather as a starting example for implementing ComponentBuilderBase.
 */
public final class CustomComponentBuilder extends ComponentBuilderBase<CustomComponentBuilder.CustomComponent> {

    @Override
    public CustomComponent build(Configuration configuration, PrintStream out, PrintStream err) {
        final JettyConfig jettyConfig = JettyConfig.buildFromConfig(configuration).build();
        return DaggerCustomComponentBuilder_CustomComponent.builder()
                .withOut(out)
                .withErr(err)
                .withJettyConfig(jettyConfig)
                // Bind CustomApplication1 directly
                .withCustomApplication1(
                        new CustomApplication1(configuration.getStringWithDefault("app1.value", "hello, world")))
                .build();
    }

    // Dagger will generate DaggerCustomComponentBuilder_CustomComponent at compile time based on the annotations
    // attached to CustomComponent and CustomComponent.Builder.
    @Singleton
    @Component(modules = {
            JettyServerModule.class,
            CommunityDefaultsModule.class,
            CustomModule.class,
    })
    public interface CustomComponent extends JettyServerComponent {

        @Component.Builder
        interface Builder extends JettyServerComponent.Builder<Builder, CustomComponent> {
            // Use @BindsInstance annotation for supplying CustomApplication1 directly
            @BindsInstance
            Builder withCustomApplication1(CustomApplication1 app1);
        }
    }

    @Module
    public interface CustomModule {

        // Use @Provides annotation for CustomApplication2
        @Provides
        static CustomApplication2 providesApp2() {
            final int value = Configuration.getInstance().getIntegerWithDefault("app2.value", 42);
            return new CustomApplication2(value);
        }

        // Register CustomApplication1 as an application
        @Binds
        @IntoSet
        ApplicationState.Factory providesApplication1(CustomApplication1 app1);

        // Register CustomApplication2 as an application
        @Binds
        @IntoSet
        ApplicationState.Factory providesApplication2(CustomApplication2 app2);

        // Register CustomAuthorization as the authorization provider
        @Binds
        AuthorizationProvider bindsAuthorizationProvider(CustomAuthorization customAuthorization);
    }
}
