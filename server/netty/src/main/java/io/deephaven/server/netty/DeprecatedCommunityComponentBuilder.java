/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.netty.DeprecatedCommunityComponentBuilder.CommunityComponent;
import io.deephaven.server.runner.CommunityDefaultsModule;
import io.deephaven.server.runner.ComponentBuilderBase;
import io.deephaven.server.runner.DeephavenApiServerComponent;

import javax.inject.Singleton;
import java.io.PrintStream;

/**
 * @deprecated see io.deephaven.server.jetty.CommunityComponentBuilder
 */
@SuppressWarnings("DeprecatedIsStillUsed")
@Deprecated
public final class DeprecatedCommunityComponentBuilder extends ComponentBuilderBase<CommunityComponent> {

    @Override
    public CommunityComponent build(Configuration configuration, PrintStream out, PrintStream err) {
        final NettyConfig nettyConfig = NettyConfig.buildFromConfig(configuration).build();
        return DaggerDeprecatedCommunityComponentBuilder_CommunityComponent.builder()
                .withNettyConfig(nettyConfig)
                .withOut(out)
                .withErr(err)
                .build();
    }

    @Deprecated
    @Singleton
    @Component(modules = {
            NettyServerModule.class,
            CommunityDefaultsModule.class,
            CommunityAuthorizationModule.class,
    })
    public interface CommunityComponent extends DeephavenApiServerComponent {

        @Component.Builder
        interface InnerBuilder extends DeephavenApiServerComponent.Builder<InnerBuilder, CommunityComponent> {
            @BindsInstance
            InnerBuilder withNettyConfig(NettyConfig config);
        }
    }
}
