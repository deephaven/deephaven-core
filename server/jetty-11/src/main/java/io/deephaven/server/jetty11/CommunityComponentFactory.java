//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import dagger.Component;
import dagger.Module;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.runner.CommunityDefaultsModule;
import io.deephaven.server.runner.ComponentFactoryBase;

import javax.inject.Singleton;
import java.io.PrintStream;

/**
 * The out-of-the-box {@link CommunityComponent} factory for the Deephaven community server.
 *
 * <p>
 * To use this directly, a main class can be configured as follows:
 *
 * <pre>
 * public final class MyMainClass {
 *     public static void main(String[] args)
 *             throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
 *         final Configuration configuration = MainHelper.init(args, MyMainClass.class);
 *         new CommunityComponentFactory()
 *                 .build(configuration)
 *                 .getServer()
 *                 .run()
 *                 .join();
 *     }
 * }
 * </pre>
 *
 * Advanced integrators should prefer to create their own component factory that extends {@link ComponentFactoryBase}.
 */
public final class CommunityComponentFactory
        extends ComponentFactoryBase<CommunityComponentFactory.CommunityComponent> {

    @Override
    public CommunityComponent build(Configuration configuration, PrintStream out, PrintStream err) {
        final JettyConfig jettyConfig = JettyConfig.buildFromConfig(configuration).build();
        return DaggerCommunityComponentFactory_CommunityComponent.builder()
                .withOut(out)
                .withErr(err)
                .withJettyConfig(jettyConfig)
                .build();
    }

    /**
     * The out-of-the-box community {@link Component}. Includes the {@link CommunityModule}.
     */
    @Singleton
    @Component(modules = CommunityModule.class)
    public interface CommunityComponent extends JettyServerComponent {

        @Component.Builder
        interface Builder extends JettyServerComponent.Builder<Builder, CommunityComponent> {
        }
    }

    /**
     * The out-of-the-box community {@link Module}.
     *
     * @see JettyServerModule
     * @see CommunityAuthorizationModule
     * @see CommunityDefaultsModule
     */
    @Module(includes = {
            JettyServerModule.class,
            JettyClientChannelFactoryModule.class,
            CommunityAuthorizationModule.class,
            CommunityDefaultsModule.class,
            // Implementation note: when / if modules are migrated out of CommunityDefaultsModule, they will need to be
            // re-added here.
    })
    public interface CommunityModule {
    }
}
