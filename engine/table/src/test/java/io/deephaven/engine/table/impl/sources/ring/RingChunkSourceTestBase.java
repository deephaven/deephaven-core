//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.ring;

import static org.assertj.core.api.Assertions.assertThat;

public class RingChunkSourceTestBase {

    public static void checkRange(AbstractRingChunkSource<?, ?, ?> ring, long expectedFirst, int expectedSize) {
        final long expectedLast = expectedFirst + expectedSize - 1;
        if (expectedLast == AbstractRingChunkSource.LAST_KEY_EMPTY) {
            if (expectedFirst != AbstractRingChunkSource.FIRST_KEY_EMPTY) {
                throw new IllegalStateException();
            }
            assertThat(ring.isEmpty()).isTrue();
        } else {
            assertThat(ring.isEmpty()).isFalse();
            assertThat(ring.containsRange(expectedFirst, expectedLast)).isTrue();
        }
        assertThat(ring.size()).isEqualTo(expectedSize);
        assertThat(ring.firstKey()).isEqualTo(expectedFirst);
        assertThat(ring.lastKey()).isEqualTo(expectedLast);
        for (long k = expectedFirst; k <= expectedLast; ++k) {
            assertThat(ring.containsKey(k)).isTrue();
            assertThat(ring.containsRange(k, k)).isTrue();
        }
        for (long k = expectedLast + 1; k < expectedLast + 100; ++k) {
            assertThat(ring.containsKey(k)).isFalse();
            assertThat(ring.containsRange(expectedFirst, k)).isFalse();
            assertThat(ring.containsRange(expectedLast, k)).isFalse();
        }
        for (long k = expectedFirst - 1; k > expectedFirst - 100; --k) {
            assertThat(ring.containsKey(k)).isFalse();
            assertThat(ring.containsRange(k, expectedFirst)).isFalse();
            assertThat(ring.containsRange(k, expectedLast)).isFalse();
        }
    }
}
