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
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Test;

import javax.inject.Singleton;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void jsPluginsManifest() throws Exception {
        // TODO(deephaven-core#3004): Jetty HTTP testing
        // Would be good to move this to testing setup with minimal overhead, where things can easily be injected and
        // changed.
        final HttpClient client = new HttpClient();
        try {
            client.start();
            final ContentResponse response =
                    client.GET(String.format("http://localhost:%d/js-plugins/manifest.json", port));
            assertThat(response.getStatus()).isEqualTo(200);
            assertThat(response.getContentAsString()).isEqualTo("{\"plugins\":[]}");
        } finally {
            client.stop();
        }
    }
}
