package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldUriTest {

    @Test
    void check1() {
        check("dh:///field/fieldName", FieldUri.of("fieldName"));
    }

    private static void check(String uriStr, FieldUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(FieldUri.of(URI.create(uriStr))).isEqualTo(uri);
    }
}
