/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.netty.NettyMain.NettyServerComponent;
import io.deephaven.server.netty.NettyMain.NettyServerComponent.Builder;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.MainBase;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public final class NettyMain extends MainBase<NettyMain, Builder, NettyServerComponent> {

    @Singleton
    @Component(modules = {
            DefaultsModule.class,
            NettyServerModule.class,
            CommunityAuthorizationModule.class,
    })
    public interface NettyServerComponent extends DeephavenApiServerComponent {
        @Component.Builder
        interface Builder extends DeephavenApiServerComponent.Builder<Builder, NettyServerComponent> {
            @BindsInstance
            Builder withNettyConfig(NettyConfig config);
        }
    }

    @Override
    public Builder builderFrom(Configuration config) {
        final NettyConfig nettyConfig = NettyConfig.buildFromConfig(config).build();
        return DaggerNettyMain_NettyServerComponent
                .builder()
                .withNettyConfig(nettyConfig);
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        new NettyMain().main(args, NettyMain.class);
    }
}
