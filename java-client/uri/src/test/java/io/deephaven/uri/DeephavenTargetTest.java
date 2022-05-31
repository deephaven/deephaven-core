package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DeephavenTargetTest {
    @Test
    void tlsHost() {
        check("dh://host", DeephavenTarget.builder().isSecure(true).host("host").build());
    }

    @Test
    void plaintextHost() {
        check("dh+plain://host", DeephavenTarget.builder().isSecure(false).host("host").build());
    }

    @Test
    void tlsHost42() {
        check("dh://host:42", DeephavenTarget.builder().isSecure(true).host("host").port(42).build());
    }

    @Test
    void plaintextHost42() {
        check("dh+plain://host:42", DeephavenTarget.builder().isSecure(false).host("host").port(42).build());
    }

    @Test
    void badPort() {
        invalid("dh://host:-1");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host").port(-1).build());
    }

    @Test
    void badPort2() {
        invalid("dh://host:-2");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host").port(-2).build());
    }

    @Test
    void badPort3() {
        invalid("dh://host:1111111111111");
    }

    @Test
    void multiport() {
        invalid("dh://host:80:80");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host:80").build());
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host:80").port(80).build());
    }

    @Test
    void noScheme() {
        invalid("host");
    }

    @Test
    void queryParams() {
        invalid("dh://host?bad=1");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host?bad=1").build());
    }

    @Test
    void fragment() {
        invalid("dh://host#bad");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host#bad").build());
    }

    @Test
    void noHost() {
        invalid("dh://");
    }

    @Test
    void noHost2() {
        invalid("dh:///");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("").build());
    }

    @Test
    void hasSlash() {
        invalid("dh://host/");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host/").build());
    }

    @Test
    void hasPath() {
        invalid("dh://host/s/table");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("host/s/table").build());
    }

    @Test
    void hasUserInfo() {
        invalid("dh://user@host");
        invalid(() -> DeephavenTarget.builder().isSecure(true).host("user@host").build());
    }

    private static void check(String uri, DeephavenTarget target) {
        assertThat(target.toString()).isEqualTo(uri);
        assertThat(target(uri)).isEqualTo(target);
    }

    private static void invalid(String uri) {
        try {
            target(uri);
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void invalid(Supplier<DeephavenTarget> supplier) {
        try {
            supplier.get();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static DeephavenTarget target(String uri) {
        return DeephavenTarget.of(URI.create(uri));
    }
}
