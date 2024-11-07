//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.server.session.ClientChannelFactoryModule;
import io.deephaven.server.session.ClientChannelFactoryModule.UserAgent;
import io.deephaven.server.session.SslConfigModule;

import java.util.List;

/**
 * @deprecated Integrators are encouraged to provide their own {@link ClientChannelFactory}, in particular with respect
 *             to {@link BarrageSessionFactoryConfig#userAgent(List)}.
 */
@Module(includes = {
        ClientChannelFactoryModule.class,
        SslConfigModule.class,
})
@Deprecated(forRemoval = true)
public interface ClientDefaultsModule {

    @Provides
    @UserAgent
    static String providesUserAgent() {
        return BarrageSessionFactoryConfig.userAgent(List.of("deephaven-server"));
    }
}
