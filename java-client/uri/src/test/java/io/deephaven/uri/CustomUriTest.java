package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class CustomUriTest {
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
    void dhField() {
        try {
            CustomUri.of(URI.create("dh:///scope/my_table"));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void tlsField() {
        try {
            CustomUri.of(URI.create("dh://host/local/f/bad"));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void plaintextField() {
        try {
            CustomUri.of(URI.create("dh+plain://host/local/f/bad"));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void check(String uriStr) {
        final CustomUri uri = CustomUri.of(URI.create(uriStr));
        assertThat(uri.toString()).isEqualTo(uriStr);
    }
}
