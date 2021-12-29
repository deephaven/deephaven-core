package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ApplicationUriTest {

    @Test
    void check1() {
        check("dh:///app/appId/field/fieldName", ApplicationUri.of("appId", "fieldName"));
    }

    @Test
    void check2() {
        invalid("dh:///app/appId/field/fieldName/field/fieldName");
    }

    private static void check(String uriStr, ApplicationUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(ApplicationUri.of(URI.create(uriStr))).isEqualTo(uri);
    }

    private static void invalid(String uriStr) {
        try {
            ApplicationUri.of(URI.create(uriStr));
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
