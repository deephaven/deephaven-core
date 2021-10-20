package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class ApplicationUriTest {

    @Test
    void check1() {
        check("app:///appId/field/fieldName", ApplicationUri.of("appId", "fieldName"));
    }

    private static void check(String uriStr, ApplicationUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(ApplicationUri.of(URI.create(uriStr))).isEqualTo(uri);
    }
}
