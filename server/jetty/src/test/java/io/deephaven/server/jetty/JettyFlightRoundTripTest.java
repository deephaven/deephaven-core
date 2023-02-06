/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

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

public class JettyFlightRoundTripTest extends FlightMessageRoundTripTest {

    @Module
    public interface JettyTestConfig {
        @Provides
        static JettyConfig providesJettyConfig() {
            return JettyConfig.builder()
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
            JettyServerModule.class,
            JettyTestConfig.class,
            LogModule.class,
            SessionModule.class,
            TableModule.class,
            TestAuthModule.class,
    })
    public interface JettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerJettyFlightRoundTripTest_JettyTestComponent.create();
    }
}
