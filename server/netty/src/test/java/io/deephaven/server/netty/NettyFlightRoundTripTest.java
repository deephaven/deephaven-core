/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.test.AuthTestModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;

import javax.inject.Singleton;

public class NettyFlightRoundTripTest extends FlightMessageRoundTripTest {

    @Module
    public interface NettyTestConfig {
        @Provides
        static NettyConfig providesNettyConfig() {
            return NettyConfig.defaultConfig();
        }
    }

    @Singleton
    @Component(modules = {
            FlightTestModule.class,
            ArrowModule.class,
            SessionModule.class,
            AuthTestModule.class,
            NettyServerModule.class,
            NettyTestConfig.class,
            ExecutionContextUnitTestModule.class,
    })
    public interface NettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerNettyFlightRoundTripTest_NettyTestComponent.create();
    }
}
