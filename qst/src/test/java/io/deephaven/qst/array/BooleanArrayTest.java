//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.util.BooleanUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BooleanArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(BooleanArray.of(false, null, true).values()).containsExactly(
                BooleanUtils.FALSE_BOOLEAN_AS_BYTE,
                BooleanUtils.NULL_BOOLEAN_AS_BYTE,
                BooleanUtils.TRUE_BOOLEAN_AS_BYTE);
    }

    @Test
    void rawInRawOut() {
        assertThat(BooleanArray.ofUnsafe(BooleanUtils.FALSE_BOOLEAN_AS_BYTE, BooleanUtils.NULL_BOOLEAN_AS_BYTE,
                BooleanUtils.TRUE_BOOLEAN_AS_BYTE).values()).containsExactly(BooleanUtils.FALSE_BOOLEAN_AS_BYTE,
                        BooleanUtils.NULL_BOOLEAN_AS_BYTE, BooleanUtils.TRUE_BOOLEAN_AS_BYTE);
    }
}
