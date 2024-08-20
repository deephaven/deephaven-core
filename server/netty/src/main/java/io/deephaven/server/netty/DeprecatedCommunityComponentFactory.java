//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.netty;

import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.netty.DeprecatedCommunityComponentFactory.CommunityComponent;
import io.deephaven.server.runner.CommunityDefaultsModule;
import io.deephaven.server.runner.ComponentFactoryBase;
import io.deephaven.server.runner.DeephavenApiServerComponent;

import javax.inject.Singleton;
import java.io.PrintStream;

/**
 * @deprecated see io.deephaven.server.jetty.CommunityComponentFactory
 */
@SuppressWarnings("DeprecatedIsStillUsed")
@Deprecated
public final class DeprecatedCommunityComponentFactory extends ComponentFactoryBase<CommunityComponent> {

    @Override
    public CommunityComponent build(Configuration configuration, PrintStream out, PrintStream err) {
        final NettyConfig nettyConfig = NettyConfig.buildFromConfig(configuration).build();
        return DaggerDeprecatedCommunityComponentFactory_CommunityComponent.builder()
                .withNettyConfig(nettyConfig)
                .withOut(out)
                .withErr(err)
                .build();
    }

    @Deprecated
    @Singleton
    @Component(modules = CommunityModule.class)
    public interface CommunityComponent extends DeephavenApiServerComponent {

        @Component.Builder
        interface Builder extends DeephavenApiServerComponent.Builder<Builder, CommunityComponent> {
            @BindsInstance
            Builder withNettyConfig(NettyConfig config);
        }
    }

    @Deprecated
    @Module(includes = {
            NettyServerModule.class,
            NettyClientChannelFactoryModule.class,
            CommunityAuthorizationModule.class,
            CommunityDefaultsModule.class,
    })
    public interface CommunityModule {

    }
}
