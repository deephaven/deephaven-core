//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DoubleRingChunkSourceTest extends RingChunkSourceTestBase {
    private static final DoubleArraySource DATA;

    static {
        DATA = new DoubleArraySource();
        DATA.ensureCapacity(1024);
        for (int i = 0; i < 1024; ++i) {
            DATA.set(i, (double) i);
        }
    }

    @Test
    public void doubleValues() {
        final DoubleRingChunkSource ring = new DoubleRingChunkSource(5);
        checkValues(ring, 0, 0);

        append(ring, 1);
        checkValues(ring, 0, 1);

        append(ring, 2);
        checkValues(ring, 0, 3);

        append(ring, 2);
        checkValues(ring, 0, 5);

        append(ring, 1);
        checkValues(ring, 1, 5);

        append(ring, 1);
        checkValues(ring, 2, 5);

        append(ring, 5);
        checkValues(ring, 7, 5);

        append(ring, 10);
        checkValues(ring, 17, 5);

        append(ring, 83);
        checkValues(ring, 100, 5);
    }

    public void checkValues(DoubleRingChunkSource source, long expectedFirst, int expectedSize) {
        checkRange(source, expectedFirst, expectedSize);
        for (long i = 0; i < expectedSize; ++i) {
            final long key = expectedFirst + i;
            assertThat(source.getDouble(key)).isEqualTo((double) key);
        }
    }

    public void append(DoubleRingChunkSource source, int len) {
        source.appendUnbounded(DATA, RowSetFactory.fromRange(source.lastKey() + 1, source.lastKey() + len));
    }
}
