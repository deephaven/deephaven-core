package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CharArrayTest {

    @Test
    void raw() {
        assertThat(CharArray.of('1', null, '3').values()).containsExactly('1', CharArray.NULL_REPR,
            '3');
    }
}
