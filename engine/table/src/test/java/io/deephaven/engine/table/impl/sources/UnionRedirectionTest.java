//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import org.junit.Test;

import static io.deephaven.engine.table.impl.sources.UnionRedirection.ALLOCATION_UNIT_ROW_KEYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link UnionRedirection}'s key space arithmetic.
 */
public class UnionRedirectionTest {

    @Test
    public void keySpaceCoversTheLastRowKey() {
        for (final long lastRowKey : new long[] {-1, 0, 1, ALLOCATION_UNIT_ROW_KEYS - 1, ALLOCATION_UNIT_ROW_KEYS,
                ALLOCATION_UNIT_ROW_KEYS + 1, 1_000_000L, Integer.MAX_VALUE, 1L << 40}) {
            final long keySpace = UnionRedirection.keySpaceFor(lastRowKey);
            assertThat(keySpace).as("keySpaceFor(%d)", lastRowKey).isGreaterThan(lastRowKey);
            assertThat(keySpace % ALLOCATION_UNIT_ROW_KEYS).as("keySpaceFor(%d)", lastRowKey).isEqualTo(0);
        }
    }

    @Test
    public void emptyTablesGetOneAllocationUnit() {
        assertThat(UnionRedirection.keySpaceFor(-1)).isEqualTo(ALLOCATION_UNIT_ROW_KEYS);
        assertThat(UnionRedirection.keySpaceFor(0)).isEqualTo(ALLOCATION_UNIT_ROW_KEYS);
    }

    /**
     * Only an empty table has a negative last row key; however negative, it gets one unit rather than an overflow
     * error.
     */
    @Test
    public void negativeLastRowKeysGetOneAllocationUnit() {
        for (final long lastRowKey : new long[] {-2, -ALLOCATION_UNIT_ROW_KEYS, -2 * ALLOCATION_UNIT_ROW_KEYS,
                Long.MIN_VALUE}) {
            assertThat(UnionRedirection.keySpaceFor(lastRowKey)).as("keySpaceFor(%d)", lastRowKey)
                    .isEqualTo(ALLOCATION_UNIT_ROW_KEYS);
        }
    }

    /**
     * The largest key space that fits in a long is the last full allocation unit below {@code Long.MAX_VALUE}.
     */
    @Test
    public void largestRepresentableKeySpace() {
        final long largestKeySpace = (Long.MAX_VALUE / ALLOCATION_UNIT_ROW_KEYS) * ALLOCATION_UNIT_ROW_KEYS;
        assertThat(UnionRedirection.keySpaceFor(largestKeySpace - 1)).isEqualTo(largestKeySpace);
    }

    /**
     * A last row key that needs more key space than a long can hold must be rejected, not wrapped to a negative size.
     */
    @Test
    public void overflowingKeySpaceIsRejected() {
        final long largestKeySpace = (Long.MAX_VALUE / ALLOCATION_UNIT_ROW_KEYS) * ALLOCATION_UNIT_ROW_KEYS;
        for (final long lastRowKey : new long[] {largestKeySpace, largestKeySpace + 1, Long.MAX_VALUE - 1,
                Long.MAX_VALUE}) {
            assertThatThrownBy(() -> UnionRedirection.keySpaceFor(lastRowKey))
                    .as("keySpaceFor(%d)", lastRowKey)
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
