package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(IntArray.of(1, null, 3).values()).containsExactly(1, Util.NULL_INT, 3);
    }

    @Test
    void rawInRawOut() {
        assertThat(IntArray.ofUnsafe(1, Util.NULL_INT, 3).values()).containsExactly(1,
                Util.NULL_INT, 3);
    }
}
