package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UriHelperTest {
    @Test
    void safe1() {
        isSafe("some-string_with1234");
    }

    @Test
    void unsafeSlash() {
        isUnsafe("/some/path/like/string");
    }

    @Test
    void unsafeBackslash() {
        isUnsafe("\\this\\is\\not\\safe");
    }

    @Test
    void unsafeSpaces() {
        isUnsafe("anything with spaces");
    }

    @Test
    void unsafePound() {
        isUnsafe("pound#pound");
    }

    @Test
    void unsafeQuestion() {
        isUnsafe("question?question");
    }

    @Test
    void unsafeAmpersand() {
        isUnsafe("amper&amper");
    }

    private static void isSafe(String x) {
        assertThat(UriHelper.isUriSafe(x)).isTrue();
    }

    private static void isUnsafe(String x) {
        assertThat(UriHelper.isUriSafe(x)).isFalse();
    }
}
