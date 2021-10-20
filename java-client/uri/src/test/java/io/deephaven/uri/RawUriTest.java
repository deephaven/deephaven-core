package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class RawUriTest {
    @Test
    void someProtocol() {
        check("some-protocol://test@host/for/example?query=yes");
    }

    @Test
    void anotherProtocol() {
        check("what:is-this#fragment");
    }

    @Test
    void file() {
        check("file:///some/file");
    }

    @Test
    void localField() {
        try {
            RawUri.of(URI.create("field:///bad"));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void tlsField() {
        try {
            RawUri.of(URI.create("dh://host/local/f/bad"));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void plaintextField() {
        try {
            RawUri.of(URI.create("dh+plain://host/local/f/bad"));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void check(String uriStr) {
        final RawUri uri = RawUri.of(URI.create(uriStr));
        assertThat(uri.toString()).isEqualTo(uriStr);
    }
}
