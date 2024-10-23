//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.server.session.ClientChannelFactoryModule;
import io.deephaven.server.session.ClientChannelFactoryModule.UserAgent;
import io.deephaven.server.session.SslConfigModule;

import java.util.List;

@Module(includes = {
        ClientChannelFactoryModule.class,
        SslConfigModule.class
})
public interface JettyClientChannelFactoryModule {

    @Provides
    @UserAgent
    static String providesUserAgent() {
        return BarrageSessionFactoryConfig.userAgent(List.of("deephaven-server-jetty"));
    }
}
