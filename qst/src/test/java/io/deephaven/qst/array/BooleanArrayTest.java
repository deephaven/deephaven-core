package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BooleanArrayTest {
    @Test
    void boxed() {
        assertThat(BooleanArray.of(false, null, true)).containsExactly(false, null, true);
    }

    @Test
    void raw() {
        assertThat(BooleanArray.of(false, null, true).values()).containsExactly(
            BooleanArray.FALSE_REPR, BooleanArray.NULL_REPR, BooleanArray.TRUE_REPR);
    }
}
