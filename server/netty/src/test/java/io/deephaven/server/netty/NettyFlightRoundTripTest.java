/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.test.TestAuthModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;

import javax.inject.Singleton;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class NettyFlightRoundTripTest extends FlightMessageRoundTripTest {

    @Module
    public interface NettyTestConfig {
        @Provides
        static NettyConfig providesNettyConfig() {
            return NettyConfig.builder()
                    .port(0)
                    .tokenExpire(Duration.of(5, ChronoUnit.MINUTES))
                    .build();
        }
    }

    @Singleton
    @Component(modules = {
            ArrowModule.class,
            ConsoleModule.class,
            ExecutionContextUnitTestModule.class,
            FlightTestModule.class,
            LogModule.class,
            NettyServerModule.class,
            NettyTestConfig.class,
            SessionModule.class,
            TableModule.class,
            TestAuthModule.class,
    })
    public interface NettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerNettyFlightRoundTripTest_NettyTestComponent.create();
    }
}
