//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DoubleArrayTest {

    @Test
    void boxInRawOut() {
        assertThat(DoubleArray.of(1d, null, 3d).values()).containsExactly(1d, QueryConstants.NULL_DOUBLE, 3d);
    }

    @Test
    void rawInRawOut() {
        assertThat(DoubleArray.ofUnsafe(1d, QueryConstants.NULL_DOUBLE, 3d).values()).containsExactly(1d,
                QueryConstants.NULL_DOUBLE, 3d);
    }

}
