package io.deephaven.qst.array;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DoubleArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(DoubleArray.of(1d, null, 3d).values()).containsExactly(1d, Util.NULL_DOUBLE, 3d);
    }

    @Test
    void rawInRawOut() {
        assertThat(DoubleArray.ofUnsafe(1d, Util.NULL_DOUBLE, 3d).values()).containsExactly(1d,
                Util.NULL_DOUBLE, 3d);
    }

}
