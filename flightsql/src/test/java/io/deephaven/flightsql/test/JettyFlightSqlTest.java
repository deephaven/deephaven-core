//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.flightsql.test;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.jetty.JettyServerModule;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;

import javax.inject.Singleton;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class JettyFlightSqlTest extends FlightSqlTest {
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
            ExecutionContextUnitTestModule.class,
            FlightTestModule.class,
            JettyServerModule.class,
            JettyTestConfig.class,
    })
    public interface JettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerJettyFlightSqlTest_JettyTestComponent.create();
    }

}
