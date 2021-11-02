package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryScopeUriTest {

    @Test
    void check1() {
        check("dh:///scope/my_table", QueryScopeUri.of("my_table"));
    }

    private static void check(String uriStr, QueryScopeUri uri) {
        assertThat(uri.toString()).isEqualTo(uriStr);
        assertThat(QueryScopeUri.of(URI.create(uriStr))).isEqualTo(uri);
    }
}
