package io.deephaven.server.jetty;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class JettyConfigTest {

    @Test
    public void defaultImplicit() throws IOException {
        check("default-implicit.json", JettyConfig.defaultConfig());
    }

    @Test
    public void defaultExplicit() throws IOException {
        check("default-explicit.json", JettyConfig.defaultConfig());
    }

    private static void check(String resource, JettyConfig expected) throws IOException {
        assertThat(JettyConfig.parseJson(JettyConfigTest.class.getResource(resource))).isEqualTo(expected);
    }
}
