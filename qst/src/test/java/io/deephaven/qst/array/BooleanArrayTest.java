package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BooleanArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(BooleanArray.of(false, null, true).values()).containsExactly(Util.FALSE_BOOL,
                Util.NULL_BOOL, Util.TRUE_BOOL);
    }

    @Test
    void rawInRawOut() {
        assertThat(BooleanArray.ofUnsafe(Util.FALSE_BOOL, Util.NULL_BOOL, Util.TRUE_BOOL).values())
                .containsExactly(Util.FALSE_BOOL, Util.NULL_BOOL, Util.TRUE_BOOL);
    }
}
