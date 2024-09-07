//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.jetty.js.Example123Registration;
import io.deephaven.server.jetty.js.Sentinel;
import io.deephaven.server.plugin.js.JsPluginsManifestRegistration;
import io.deephaven.server.plugin.js.JsPluginsNpmPackageRegistration;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;

import javax.inject.Singleton;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

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
            ExecutionContextUnitTestModule.class,
            FlightTestModule.class,
            JettyServerModule.class,
            JettyTestConfig.class,
    })
    public interface JettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerJettyFlightRoundTripTest_JettyTestComponent.create();
    }

    @Test
    public void jsPlugins() throws Exception {
        // Note: JettyFlightRoundTripTest is not the most minimal / appropriate bootstrapping for this test, but it is
        // the most convenient since it has all of the necessary prerequisites
        new Example123Registration().registerInto(component.registration());
        testJsPluginExamples(false, true, true);
    }

    @Test
    public void jsPluginsFromManifest() throws Exception {
        // Note: JettyFlightRoundTripTest is not the most minimal / appropriate bootstrapping for this test, but it is
        // the most convenient since it has all of the necessary prerequisites
        final Path manifestRoot = Path.of(Sentinel.class.getResource("examples").toURI());
        new JsPluginsManifestRegistration(manifestRoot)
                .registerInto(component.registration());
        testJsPluginExamples(false, false, true);
    }

    @Test
    public void jsPluginsFromNpmPackages() throws Exception {
        // Note: JettyFlightRoundTripTest is not the most minimal / appropriate bootstrapping for this test, but it is
        // the most convenient since it has all of the necessary prerequisites
        final Path example1Root = Path.of(Sentinel.class.getResource("examples/@deephaven_test/example1").toURI());
        final Path example2Root = Path.of(Sentinel.class.getResource("examples/@deephaven_test/example2").toURI());
        // example3 is *not* a npm package, no package.json.
        new JsPluginsNpmPackageRegistration(example1Root)
                .registerInto(component.registration());
        new JsPluginsNpmPackageRegistration(example2Root)
                .registerInto(component.registration());
        testJsPluginExamples(true, true, false);
    }

    private void testJsPluginExamples(boolean example1IsLimited, boolean example2IsLimited, boolean hasExample3)
            throws Exception {
        final HttpClient client = new HttpClient();
        client.start();
        try {
            if (hasExample3) {
                manifestTest123(client);
            } else {
                manifestTest12(client);
            }
            example1Tests(client, example1IsLimited);
            example2Tests(client, example2IsLimited);
            if (hasExample3) {
                example3Tests(client);
            }
        } finally {
            client.stop();
        }
    }

    private void manifestTest12(HttpClient client) throws InterruptedException, TimeoutException, ExecutionException {
        final ContentResponse manifestResponse = get(client, "js-plugins/manifest.json");
        assertOk(manifestResponse, "application/json",
                "{\"plugins\":[{\"name\":\"@deephaven_test/example1\",\"version\":\"0.1.0\",\"main\":\"dist/index.js\"},{\"name\":\"@deephaven_test/example2\",\"version\":\"0.2.0\",\"main\":\"dist/index.js\"}]}");
    }

    private void manifestTest123(HttpClient client) throws InterruptedException, TimeoutException, ExecutionException {
        final ContentResponse manifestResponse = get(client, "js-plugins/manifest.json");
        assertOk(manifestResponse, "application/json",
                "{\"plugins\":[{\"name\":\"@deephaven_test/example1\",\"version\":\"0.1.0\",\"main\":\"dist/index.js\"},{\"name\":\"@deephaven_test/example2\",\"version\":\"0.2.0\",\"main\":\"dist/index.js\"},{\"name\":\"@deephaven_test/example3\",\"version\":\"0.3.0\",\"main\":\"index.js\"}]}");
    }

    private void example1Tests(HttpClient client, boolean isLimited)
            throws InterruptedException, TimeoutException, ExecutionException {
        if (isLimited) {
            assertThat(get(client, "js-plugins/@deephaven_test/example1/package.json").getStatus())
                    .isEqualTo(HttpStatus.NOT_FOUND_404);
        } else {
            assertOk(get(client, "js-plugins/@deephaven_test/example1/package.json"),
                    "application/json",
                    "{\"name\":\"@deephaven_test/example1\",\"version\":\"0.1.0\",\"main\":\"dist/index.js\",\"files\":[\"dist\"]}");
        }

        assertOk(
                get(client, "js-plugins/@deephaven_test/example1/dist/index.js"),
                "text/javascript",
                "// example1/dist/index.js");

        assertOk(
                get(client, "js-plugins/@deephaven_test/example1/dist/index2.js"),
                "text/javascript",
                "// example1/dist/index2.js");
    }

    private void example2Tests(HttpClient client, boolean isLimited)
            throws InterruptedException, TimeoutException, ExecutionException {
        if (isLimited) {
            assertThat(get(client, "js-plugins/@deephaven_test/example2/package.json").getStatus())
                    .isEqualTo(HttpStatus.NOT_FOUND_404);
        } else {
            assertOk(get(client, "js-plugins/@deephaven_test/example2/package.json"),
                    "application/json",
                    "{\"name\":\"@deephaven_test/example2\",\"version\":\"0.2.0\",\"main\":\"dist/index.js\",\"files\":[\"dist\"]}");
        }

        assertOk(
                get(client, "js-plugins/@deephaven_test/example2/dist/index.js"),
                "text/javascript",
                "// example2/dist/index.js");

        assertOk(
                get(client, "js-plugins/@deephaven_test/example2/dist/index2.js"),
                "text/javascript",
                "// example2/dist/index2.js");
    }

    private void example3Tests(HttpClient client) throws InterruptedException, TimeoutException, ExecutionException {
        assertOk(
                get(client, "js-plugins/@deephaven_test/example3/index.js"),
                "text/javascript",
                "// example3/index.js");
    }

    private ContentResponse get(HttpClient client, String path)
            throws InterruptedException, TimeoutException, ExecutionException {
        return client
                .newRequest("localhost", localPort)
                .path(path)
                .method(HttpMethod.GET)
                .send();
    }

    private static void assertOk(ContentResponse response, String contentType, String expected) {
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
        assertThat(response.getMediaType()).isEqualTo(contentType);
        assertThat(response.getContentAsString()).isEqualTo(expected);
        assertNoCache(response);
    }

    private static void assertNoCache(ContentResponse response) {
        final HttpFields headers = response.getHeaders();
        assertThat(headers.getDateField("Expires")).isEqualTo(0);
        assertThat(headers.get("Pragma")).isEqualTo("no-cache");
        assertThat(headers.get("Cache-control")).isEqualTo("no-cache, must-revalidate, pre-check=0, post-check=0");
    }
}
