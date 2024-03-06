//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IntArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(IntArray.of(1, null, 3).values()).containsExactly(1, QueryConstants.NULL_INT, 3);
    }

    @Test
    void rawInRawOut() {
        assertThat(IntArray.ofUnsafe(1, QueryConstants.NULL_INT, 3).values()).containsExactly(1,
                QueryConstants.NULL_INT, 3);
    }
}
