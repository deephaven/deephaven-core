package io.deephaven.server.netty;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyConfigTest {

    @Test
    public void defaultImplicit() throws IOException {
        check("default-implicit.json", NettyConfig.defaultConfig());
    }

    @Test
    public void defaultExplicit() throws IOException {
        check("default-explicit.json", NettyConfig.defaultConfig());
    }

    private static void check(String resource, NettyConfig expected) throws IOException {
        assertThat(NettyConfig.parseJson(NettyConfigTest.class.getResource(resource))).isEqualTo(expected);
    }
}
