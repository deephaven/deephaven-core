package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class DeephavenTargetTest {
    @Test
    void tlsHost() {
        check("dh://host", DeephavenTarget.builder().isTLS(true).host("host").build());
    }

    @Test
    void plaintextHost() {
        check("dh+plain://host", DeephavenTarget.builder().isTLS(false).host("host").build());
    }

    @Test
    void tlsHost42() {
        check("dh://host:42", DeephavenTarget.builder().isTLS(true).host("host").port(42).build());
    }

    @Test
    void plaintextHost42() {
        check("dh+plain://host:42", DeephavenTarget.builder().isTLS(false).host("host").port(42).build());
    }

    @Test
    void noScheme() {
        try {
            target("host");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void queryParams() {
        try {
            target("dh://host?bad=1");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void fragment() {
        try {
            target("dh://host#bad");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void noHost() {
        try {
            target("dh://");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void noHost2() {
        try {
            target("dh:///");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void hasSlash() {
        try {
            target("dh://host/");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void hasPath() {
        try {
            target("dh://host/s/table");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void hasUserInfo() {
        try {
            target("dh://user@host");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void check(String uri, DeephavenTarget target) {
        assertThat(target.toString()).isEqualTo(uri);
        assertThat(target(uri)).isEqualTo(target);
    }

    private static DeephavenTarget target(String uri) {
        return DeephavenTarget.of(URI.create(uri));
    }
}
