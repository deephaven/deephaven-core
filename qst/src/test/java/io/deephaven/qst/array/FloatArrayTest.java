package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FloatArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(FloatArray.of(1f, null, 3f).values()).containsExactly(1f, Util.NULL_FLOAT, 3f);
    }

    @Test
    void rawInRawOut() {
        assertThat(FloatArray.ofUnsafe(1f, Util.NULL_FLOAT, 3f).values()).containsExactly(1f,
                Util.NULL_FLOAT, 3f);
    }
}
