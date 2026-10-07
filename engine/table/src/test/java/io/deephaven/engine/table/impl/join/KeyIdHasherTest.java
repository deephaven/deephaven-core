//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

public class KeyIdHasherTest extends RefreshingTableTestCase {

    @Test
    public void testSingleKey() {
        // one key column uses a pregenerated hasher
        for (int seed = 0; seed < 5; ++seed) {
            testRandom(new Random(seed), KeyIdHasherTest::singleKeyTable);
        }
    }

    @Test
    public void testTwoKeys() {
        // two key columns use a hasher compiled at runtime
        for (int seed = 0; seed < 3; ++seed) {
            testRandom(new Random(seed), KeyIdHasherTest::twoKeyTable);
        }
    }

    private static Table singleKeyTable(final Random random, final int step) {
        // later steps draw from more keys, so that the table keeps growing
        final int size = random.nextInt(20_000);
        final long[] keys = new long[size];
        for (int ii = 0; ii < size; ++ii) {
            keys[ii] = random.nextInt(2_000 + 4_000 * step);
        }
        return newTable(longCol("K", keys));
    }

    private static Table twoKeyTable(final Random random, final int step) {
        final int size = random.nextInt(6_000);
        final int[] ints = new int[size];
        final String[] strings = new String[size];
        for (int ii = 0; ii < size; ++ii) {
            ints[ii] = random.nextInt(40);
            strings[ii] = random.nextInt(50) == 0 ? null : Integer.toString(random.nextInt(50));
        }
        return newTable(intCol("I", ints), stringCol("S", strings));
    }

    @FunctionalInterface
    private interface TableMaker {
        Table make(Random random, int step);
    }

    /**
     * Build random batches of keys, checking each key's id against a reference map.
     */
    private static void testRandom(final Random random, final TableMaker tableMaker) {
        final Table prototype = tableMaker.make(random, 0);
        final KeyIdHasher hasher = KeyIdHasherTypedBase.make(sources(prototype), 16, 0.75);

        final Map<List<Object>, Integer> expected = new HashMap<>();
        for (int step = 0; step < 50; ++step) {
            final Table toBuild = tableMaker.make(random, step);
            final ColumnSource<?>[] sources = sources(toBuild);
            hasher.build(toBuild.getRowSet(), sources, (rows, ids) -> {
                final int[] position = {0};
                rows.forAllRowKeys(rowKey -> {
                    final List<Object> key = keyOf(sources, rowKey);
                    final int id = ids.get(position[0]++);
                    assertNotEquals(KeyIdHasher.NULL_ID, id);
                    final Integer existing = expected.putIfAbsent(key, id);
                    if (existing != null) {
                        assertEquals((int) existing, id);
                    }
                });
            });
            assertEquals(expected.size(), hasher.size());
            // the ids are dense
            assertEquals(expected.size(), hasher.idCapacity());
            assertEquals(expected.size(), new IntOpenHashSet(expected.values()).size());

            // every key that was built probes to its id
            hasher.probe(toBuild.getRowSet(), sources, false, (rows, ids) -> {
                final int[] position = {0};
                rows.forAllRowKeys(rowKey -> assertEquals((int) expected.get(keyOf(sources, rowKey)),
                        ids.get(position[0]++)));
            });
        }

        // a key that was never built probes to NULL_ID
        final Table missing = missingKeyTable(prototype);
        hasher.probe(missing.getRowSet(), sources(missing), false,
                (rows, ids) -> assertEquals(KeyIdHasher.NULL_ID, ids.get(0)));
    }

    private static ColumnSource<?>[] sources(final Table table) {
        return table.getColumnSources().toArray(ColumnSource<?>[]::new);
    }

    private static List<Object> keyOf(final ColumnSource<?>[] sources, final long rowKey) {
        final List<Object> key = new ArrayList<>(sources.length);
        for (final ColumnSource<?> source : sources) {
            key.add(source.get(rowKey));
        }
        return key;
    }

    private static Table missingKeyTable(final Table prototype) {
        final String[] names = prototype.getDefinition().getColumnNamesArray();
        if (names.length == 1) {
            return newTable(longCol(names[0], -1));
        }
        return newTable(intCol(names[0], -1), stringCol(names[1], "missing"));
    }
}
