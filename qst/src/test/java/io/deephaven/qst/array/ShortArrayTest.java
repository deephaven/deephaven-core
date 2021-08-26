package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ShortArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(ShortArray.of((short) 1, null, (short) 3).values()).containsExactly((short) 1,
                Util.NULL_SHORT, (short) 3);
    }

    @Test
    void rawInRawOut() {
        assertThat(ShortArray.ofUnsafe((short) 1, Util.NULL_SHORT, (short) 3).values())
                .containsExactly((short) 1, Util.NULL_SHORT, (short) 3);
    }
}
