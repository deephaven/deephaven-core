/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.test.AuthTestModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;

import javax.inject.Singleton;

public class JettyFlightRoundTripTest extends FlightMessageRoundTripTest {

    @Module
    public interface JettyTestConfig {
        @Provides
        static JettyConfig providesJettyConfig() {
            return JettyConfig.defaultConfig();
        }
    }

    @Singleton
    @Component(modules = {
            FlightTestModule.class,
            ArrowModule.class,
            SessionModule.class,
            AuthTestModule.class,
            JettyServerModule.class,
            JettyTestConfig.class,
            ExecutionContextUnitTestModule.class,
    })
    public interface JettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerJettyFlightRoundTripTest_JettyTestComponent.create();
    }
}
