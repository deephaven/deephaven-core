//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBitmapRandomBuilder {
    @Test
    public void testReuseWithExclusions() {
        final Random random = new Random(0);
        final BitmapRandomBuilder builder = new BitmapRandomBuilder(0);
        int maxKey = 100;
        for (int round = 0; round < 200; ++round) {
            // the maximum key rises from one use to the next, as an aggregation's output positions do
            maxKey += random.nextInt(500);
            builder.reset(maxKey);
            final RowSetBuilderRandom expectedBuilder = RowSetFactory.builderRandom();
            final int keys = random.nextInt(300);
            for (int ii = 0; ii < keys; ++ii) {
                // some keys past the maximum, which are ignored
                final long key = random.nextInt(maxKey + 64);
                builder.addKey(key);
                if (key < maxKey) {
                    expectedBuilder.addKey(key);
                }
            }
            // ranges that start and end within words, span several words, and lie partly past the maximum
            final RowSetBuilderRandom excludedBuilder = RowSetFactory.builderRandom();
            for (int ii = 0; ii < 5; ++ii) {
                final long first = random.nextInt(maxKey + 64);
                excludedBuilder.addRange(first, first + random.nextInt(200));
            }
            excludedBuilder.addKey(random.nextInt(maxKey));
            try (final WritableRowSet expected = expectedBuilder.build();
                    final RowSet excluded = excludedBuilder.build();
                    final RowSet built = round % 3 == 0 ? builder.build() : builder.build(excluded)) {
                if (round % 3 != 0) {
                    expected.remove(excluded);
                }
                assertEquals("round=" + round, expected, built);
            }
        }
    }

    @Test
    public void testDenseRuns() {
        final Random random = new Random(1);
        final BitmapRandomBuilder builder = new BitmapRandomBuilder(0);
        for (int round = 0; round < 100; ++round) {
            final int maxKey = 64 * (1 + random.nextInt(40));
            builder.reset(maxKey);
            final RowSetBuilderRandom expectedBuilder = RowSetFactory.builderRandom();
            // runs that fill whole words, cross word boundaries, and end on the last bit of a word
            for (int ii = 0; ii < 6; ++ii) {
                final int first = random.nextInt(maxKey);
                final int last = Math.min(maxKey - 1, first + random.nextInt(150));
                for (int key = first; key <= last; ++key) {
                    builder.addKey(key);
                }
                expectedBuilder.addRange(first, last);
            }
            builder.addKey(maxKey - 1);
            expectedBuilder.addKey(maxKey - 1);
            try (final RowSet expected = expectedBuilder.build();
                    final RowSet built = builder.build()) {
                assertEquals("round=" + round, expected, built);
            }
        }
    }

    @Test
    public void testResetDiscardsUnbuiltKeys() {
        final BitmapRandomBuilder builder = new BitmapRandomBuilder(1000);
        builder.addKey(5);
        builder.addKey(700);
        builder.reset(1000);
        builder.addKey(6);
        try (final RowSet built = builder.build()) {
            assertEquals(RowSetFactory.fromKeys(6), built);
        }
        try (final RowSet built = builder.build()) {
            assertTrue(built.isEmpty());
        }
    }
}
