package io.deephaven.server.jetty;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.jetty.CommunityComponentBuilder.CommunityComponent;
import io.deephaven.server.runner.CommunityDefaultsModule;
import io.deephaven.server.runner.ComponentBuilderBase;
import io.deephaven.server.runner.DeephavenApiServerComponent;

import javax.inject.Singleton;
import java.io.PrintStream;

/**
 * The out-of-the-box {@link CommunityComponent} builder for the Deephaven community server.
 *
 * <p>
 * To use this directly, a main class can be configured as follows:
 *
 * <pre>
 * public final class MyMainClass {
 *     public static void main(String[] args)
 *             throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
 *         final Configuration configuration = MainHelper.init(args, MyMainClass.class);
 *         new CommunityComponentBuilder()
 *                 .build(configuration)
 *                 .getServer()
 *                 .run()
 *                 .join();
 *     }
 * }
 * </pre>
 *
 * Advanced integrators should prefer to create their own component builder that extends {@link ComponentBuilderBase}.
 */
public final class CommunityComponentBuilder extends ComponentBuilderBase<CommunityComponent> {

    @Override
    public CommunityComponent build(Configuration configuration, PrintStream out, PrintStream err) {
        final JettyConfig jettyConfig = JettyConfig.buildFromConfig(configuration).build();
        return DaggerCommunityComponentBuilder_CommunityComponent.builder()
                .withJettyConfig(jettyConfig)
                .withOut(out)
                .withErr(err)
                .build();
    }

    /**
     * The out-of-the-box community component.
     *
     * @see JettyServerModule
     * @see CommunityDefaultsModule
     * @see CommunityAuthorizationModule
     */
    @Singleton
    @Component(modules = {
            JettyServerModule.class,
            CommunityDefaultsModule.class,
            CommunityAuthorizationModule.class,
    })
    public interface CommunityComponent extends DeephavenApiServerComponent {

        @Component.Builder
        interface InnerBuilder extends DeephavenApiServerComponent.Builder<InnerBuilder, CommunityComponent> {
            @BindsInstance
            InnerBuilder withJettyConfig(JettyConfig config);
        }
    }
}
