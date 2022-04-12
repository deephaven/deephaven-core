package io.deephaven.server.jetty;

import dagger.Component;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthContextModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;

import javax.inject.Singleton;

public class JettyFlightRoundTripTest extends FlightMessageRoundTripTest {

    @Singleton
    @Component(modules = {
            FlightTestModule.class,
            ArrowModule.class,
            SessionModule.class,
            AuthContextModule.class,
            JettyServerModule.class
    })
    public interface JettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerJettyFlightRoundTripTest_JettyTestComponent.create();
    }
}
