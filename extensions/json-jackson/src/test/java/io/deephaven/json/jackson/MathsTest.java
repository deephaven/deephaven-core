//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.base.ArrayUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MathsTest {

    @Test
    void nextPowerOf2() {
        pow2(0, 1);
        pow2(1, 1);
        pow2(2, 2);
        for (int i = 2; i < 31; ++i) {
            final int pow2 = 1 << i;
            pow2(pow2, pow2);
            pow2(pow2 - 1, pow2);
            if (i < 30) {
                pow2(pow2 + 1, pow2 * 2);
            }
        }
    }

    @Test
    void nextArrayCapacity() {
        arrayCapacity(0, 1);
        arrayCapacity(1, 1);
        arrayCapacity(2, 2);
        for (int i = 2; i < 31; ++i) {
            final int pow2 = 1 << i;
            arrayCapacity(pow2, pow2);
            arrayCapacity(pow2 - 1, pow2);
            if (i < 30) {
                arrayCapacity(pow2 + 1, pow2 * 2);
            } else {
                arrayCapacity(pow2 + 1, ArrayUtil.MAX_ARRAY_SIZE);
            }
        }
    }

    public static void pow2(int newSize, int expectedSize) {
        assertThat(Maths.nextPowerOf2(newSize)).isEqualTo(expectedSize);
    }

    public static void arrayCapacity(int newSize, int expectedSize) {
        assertThat(Maths.nextArrayCapacity(newSize)).isEqualTo(expectedSize);
    }
}
