//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LongArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(LongArray.of(1L, null, 3L).values()).containsExactly(1L, QueryConstants.NULL_LONG, 3L);
    }

    @Test
    void rawInRawOut() {
        assertThat(LongArray.ofUnsafe(1L, QueryConstants.NULL_LONG, 3L).values()).containsExactly(1L,
                QueryConstants.NULL_LONG, 3L);
    }
}
