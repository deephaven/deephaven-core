//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.join.StaticKeyLongValueHasherTypedBase.RepeatedKey;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

public class StaticKeyLongValueHasherTest extends RefreshingTableTestCase {

    private static final long MARK = -2;
    private static final long MISSING = -1;

    @Test
    public void testSingleKey() {
        // one key column uses a pregenerated hasher
        for (final RepeatedKey repeatedKey : RepeatedKey.values()) {
            for (int seed = 0; seed < 5; ++seed) {
                testRandom(new Random(seed), repeatedKey, StaticKeyLongValueHasherTest::singleKeyTable);
            }
        }
    }

    @Test
    public void testTwoKeys() {
        // two key columns use a hasher compiled at runtime
        for (final RepeatedKey repeatedKey : RepeatedKey.values()) {
            for (int seed = 0; seed < 3; ++seed) {
                testRandom(new Random(seed), repeatedKey, StaticKeyLongValueHasherTest::twoKeyTable);
            }
        }
    }

    @Test
    public void testGrowsWhenAnInsertionFillsIt() {
        // at a load factor of 0.75, a table of 4096 slots holds 3072 keys
        final StaticKeyLongValueHasherTypedBase hasher = StaticKeyLongValueHasherTypedBase.make(
                sources(newTable(longCol("K"))), 4096, 0.75, RepeatedKey.KEEP_FIRST, MARK);

        // one chunk of 4096 rows holding 3072 distinct keys fits, although the chunk has more rows than room
        final long[] keys = new long[4096];
        final long[] values = new long[4096];
        for (int ii = 0; ii < keys.length; ++ii) {
            keys[ii] = ii % 3072;
            values[ii] = 10L * ii;
        }
        // noinspection unchecked
        hasher.build(new Chunk[] {LongChunk.chunkWrap(keys)}, LongChunk.chunkWrap(values));
        assertEquals(3072, hasher.size());
        assertEquals(4096, hasher.tableSize);

        // the next key takes the table past its load factor, so it doubles before another key is probed
        // noinspection unchecked
        hasher.build(new Chunk[] {LongChunk.chunkWrap(new long[] {3072, 3073})},
                LongChunk.chunkWrap(new long[] {7, 8}));
        assertEquals(8192, hasher.tableSize);
        assertFalse(hasher.anyMarked());

        // every key keeps its first value across the rehash
        final long[] all = new long[3075];
        for (int ii = 0; ii < all.length; ++ii) {
            all[ii] = ii;
        }
        try (final WritableLongChunk<Values> found = WritableLongChunk.makeWritableChunk(all.length)) {
            // noinspection unchecked
            hasher.probe(new Chunk[] {LongChunk.chunkWrap(all)}, found, MISSING);
            for (int ii = 0; ii < 3072; ++ii) {
                assertEquals(10L * ii, found.get(ii));
            }
            assertEquals(7, found.get(3072));
            assertEquals(8, found.get(3073));
            assertEquals(MISSING, found.get(3074));
        }
    }

    @Test
    public void testCompleteRejectsBuild() {
        final Table keys = newTable(longCol("K", 5, 6, 5));
        final StaticKeyLongValueHasherTypedBase hasher =
                StaticKeyLongValueHasherTypedBase.make(sources(keys), 16, 0.75, RepeatedKey.MARK, MARK);
        hasher.build(keys.getRowSet(), sources(keys));
        hasher.complete();
        assertTrue(hasher.anyMarked());

        // probes are unaffected; the values are the row keys, and the repeated key is marked
        hasher.probe(keys.getRowSet(), sources(keys), false, MISSING, (rows, values) -> {
            assertEquals(MARK, values.get(0));
            assertEquals(1, values.get(1));
            assertEquals(MARK, values.get(2));
        });

        // a build of no rows changes nothing, so it is allowed; a build of any rows throws
        final Table none = newTable(longCol("K"));
        hasher.build(none.getRowSet(), sources(none));
        assertThrows(IllegalStateException.class, () -> hasher.build(keys.getRowSet(), sources(keys)));
        assertEquals(2, hasher.size());
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
            ints[ii] = random.nextInt(40 + 10 * step);
            strings[ii] = random.nextInt(50) == 0 ? null : Integer.toString(random.nextInt(50));
        }
        return newTable(intCol("I", ints), stringCol("S", strings));
    }

    @FunctionalInterface
    private interface TableMaker {
        Table make(Random random, int step);
    }

    /**
     * Build random batches of keys, each row's value its row key, checking every key's value against a reference map
     * that applies {@code repeatedKey} to the rows in order.
     */
    private static void testRandom(final Random random, final RepeatedKey repeatedKey, final TableMaker tableMaker) {
        final Table prototype = tableMaker.make(random, 0);
        final StaticKeyLongValueHasherTypedBase hasher =
                StaticKeyLongValueHasherTypedBase.make(sources(prototype), 16, 0.75, repeatedKey, MARK);

        final Map<List<Object>, Long> expected = new HashMap<>();
        boolean anyRepeated = false;
        for (int step = 0; step < 20; ++step) {
            final Table toBuild = tableMaker.make(random, step);
            final ColumnSource<?>[] sources = sources(toBuild);
            hasher.build(toBuild.getRowSet(), sources);
            for (long rowKey = 0; rowKey < toBuild.size(); ++rowKey) {
                final List<Object> key = keyOf(sources, rowKey);
                final Long existing = expected.get(key);
                if (existing == null) {
                    expected.put(key, rowKey);
                } else {
                    anyRepeated = true;
                    switch (repeatedKey) {
                        case KEEP_FIRST:
                            break;
                        case KEEP_LAST:
                            expected.put(key, rowKey);
                            break;
                        case MARK:
                            expected.put(key, MARK);
                            break;
                    }
                }
            }
            assertEquals(expected.size(), hasher.size());
            assertEquals(repeatedKey == RepeatedKey.MARK && anyRepeated, hasher.anyMarked());

            // every key that was built probes to its value
            hasher.probe(toBuild.getRowSet(), sources, step % 2 == 0, MISSING, (rows, values) -> {
                final int[] position = {0};
                rows.forAllRowKeys(rowKey -> assertEquals((long) expected.get(keyOf(sources, rowKey)),
                        values.get(position[0]++)));
            });
        }

        // a later step draws from more keys than were built, so some probe to the missing value
        final Table absent = tableMaker.make(new Random(-1), 30);
        final ColumnSource<?>[] absentSources = sources(absent);
        hasher.probe(absent.getRowSet(), absentSources, false, MISSING, (rows, values) -> {
            final int[] position = {0};
            rows.forAllRowKeys(rowKey -> {
                final Long value = expected.get(keyOf(absentSources, rowKey));
                assertEquals(value == null ? MISSING : value, values.get(position[0]++));
            });
        });
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
}
