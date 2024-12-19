//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.DeephavenServerTestBase.TestComponent;
import io.deephaven.server.flightsql.JettyTestComponent.JettyTestConfig;
import io.deephaven.server.jetty11.JettyConfig;
import io.deephaven.server.jetty11.JettyServerModule;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;

import javax.inject.Singleton;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Singleton
@Component(modules = {
        ExecutionContextUnitTestModule.class,
        JettyServerModule.class,
        JettyTestConfig.class,
        FlightSqlTestModule.class,
})
public interface JettyTestComponent extends TestComponent {

    @Module
    interface JettyTestConfig {
        @Provides
        static JettyConfig providesJettyConfig() {
            return JettyConfig.builder()
                    .port(0)
                    .tokenExpire(Duration.of(5, ChronoUnit.MINUTES))
                    .build();
        }
    }
}
