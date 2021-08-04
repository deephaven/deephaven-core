package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LongArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(LongArray.of(1L, null, 3L).values()).containsExactly(1L, Util.NULL_LONG, 3L);
    }

    @Test
    void rawInRawOut() {
        assertThat(LongArray.ofUnsafe(1L, Util.NULL_LONG, 3L).values()).containsExactly(1L,
            Util.NULL_LONG, 3L);
    }
}
