package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BooleanArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(BooleanArray.of(false, null, true).values()).containsExactly(
            BooleanArray.FALSE_REPR, BooleanArray.NULL_REPR, BooleanArray.TRUE_REPR);
    }

    @Test
    void rawInRawOut() {
        assertThat(BooleanArray
            .ofUnsafe(BooleanArray.FALSE_REPR, BooleanArray.NULL_REPR, BooleanArray.TRUE_REPR)
            .values()).containsExactly(BooleanArray.FALSE_REPR, BooleanArray.NULL_REPR,
                BooleanArray.TRUE_REPR);
    }
}
