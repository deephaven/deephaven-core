//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.lang3.mutable.MutableBoolean;
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
        for (final boolean incremental : new boolean[] {false, true}) {
            for (int seed = 0; seed < 5; ++seed) {
                final Random random = new Random(seed);
                final boolean sawPartialRehash = testRandom(random, incremental, KeyIdHasherTest::singleKeyTable);
                // the keys outgrow the table while an alternate table is still draining
                assertEquals(incremental, sawPartialRehash);
            }
        }
    }

    @Test
    public void testTwoKeys() {
        // two key columns use a hasher compiled at runtime
        for (final boolean incremental : new boolean[] {false, true}) {
            for (int seed = 0; seed < 3; ++seed) {
                final Random random = new Random(seed);
                testRandom(random, incremental, KeyIdHasherTest::twoKeyTable);
            }
        }
    }

    @Test
    public void testFullRehashLeavesRoom() {
        // at a load factor near one, growing to fit 8 live and 8 new keys must not produce a table of exactly 16 slots
        final Table first = newTable(longCol("K", 0, 1, 2, 3, 4, 5, 6, 7));
        final Table second = newTable(longCol("K", 8, 9, 10, 11, 12, 13, 14, 15));
        final IncrementalKeyIdHasherTypedBase hasher = IncrementalKeyIdHasherTypedBase.make(sources(first), 16, 0.95);
        hasher.buildWithFullRehash(first.getRowSet(), sources(first), (rows, ids, statuses) -> {
        });
        hasher.buildWithFullRehash(second.getRowSet(), sources(second), (rows, ids, statuses) -> {
        });
        assertEquals(16, hasher.size());

        final Table missing = newTable(longCol("K", 16));
        hasher.probe(missing.getRowSet(), sources(missing), false,
                (rows, ids, statuses) -> assertEquals(KeyIdHasher.NULL_ID, ids.get(0)));
    }

    @Test
    public void testFullRehashAfterRemoveFails() {
        // a removed key leaves a tombstone, which remains after another key reuses its id
        final Table first = newTable(longCol("K", 0, 1));
        final Table second = newTable(longCol("K", 2));
        final IncrementalKeyIdHasherTypedBase hasher = IncrementalKeyIdHasherTypedBase.make(sources(first), 16, 0.75);
        hasher.buildWithFullRehash(first.getRowSet(), sources(first), (rows, ids, statuses) -> {
        });
        hasher.remove(IntChunk.chunkWrap(new int[] {0}));
        hasher.build(second.getRowSet(), sources(second), (rows, ids, statuses) -> assertEquals(0, ids.get(0)));
        assertEquals(hasher.size(), hasher.idCapacity());
        assertThrows(AssertionFailure.class,
                () -> hasher.buildWithFullRehash(second.getRowSet(), sources(second), (rows, ids, statuses) -> {
                }));
    }

    @Test
    public void testChunkContext() {
        // build and probe key chunks directly, reusing one context across chunks while the table grows
        final IncrementalKeyIdHasherTypedBase hasher =
                IncrementalKeyIdHasherTypedBase.make(sources(newTable(longCol("K"))), 16, 0.75);
        final Map<Long, Integer> expected = new HashMap<>();
        try (final KeyIdHasher.Context context = hasher.makeContext(1024)) {
            for (int chunk = 0; chunk < 40; ++chunk) {
                final long[] keys = new long[1024];
                for (int ii = 0; ii < keys.length; ++ii) {
                    // half the keys repeat earlier chunks, half are new
                    keys[ii] = ii % 2 == 0 ? ii : chunk * 1024L + ii;
                }
                // noinspection unchecked
                final Chunk<Values>[] keyChunks = new Chunk[] {LongChunk.chunkWrap(keys)};
                hasher.build(context, keyChunks);
                for (int ii = 0; ii < keys.length; ++ii) {
                    final Integer existing = expected.putIfAbsent(keys[ii], context.ids().get(ii));
                    assertEquals(existing == null ? KeyIdHasher.ADDED : KeyIdHasher.FOUND,
                            context.statuses().get(ii));
                    if (existing != null) {
                        assertEquals((int) existing, context.ids().get(ii));
                    }
                }
                hasher.probe(context, keyChunks);
                for (int ii = 0; ii < keys.length; ++ii) {
                    assertEquals((int) expected.get(keys[ii]), context.ids().get(ii));
                    assertEquals(KeyIdHasher.FOUND, context.statuses().get(ii));
                }
            }
            // noinspection unchecked
            hasher.probe(context, new Chunk[] {LongChunk.chunkWrap(new long[] {-1, -2})});
            assertEquals(KeyIdHasher.NULL_ID, context.ids().get(0));
            assertEquals(KeyIdHasher.MISSING, context.statuses().get(1));
        }
        assertEquals(expected.size(), hasher.size());
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
     * Build random batches of keys, checking each key's id against a reference map. When {@code incremental}, also
     * remove a random subset of the keys after each batch.
     *
     * @return whether an incremental hasher was ever between chunks of a build with an alternate table still draining
     */
    private static boolean testRandom(final Random random, final boolean incremental, final TableMaker tableMaker) {
        final Table prototype = tableMaker.make(random, 0);
        final ColumnSource<?>[] prototypeSources = sources(prototype);
        final KeyIdHasher hasher = incremental
                ? IncrementalKeyIdHasherTypedBase.make(prototypeSources, 16, 0.75)
                : KeyIdHasherTypedBase.make(prototypeSources, 16, 0.75);
        final MutableBoolean sawPartialRehash = new MutableBoolean();

        final Map<List<Object>, Integer> expected = new HashMap<>();
        int maxLive = 0;
        for (int step = 0; step < 50; ++step) {
            final Table toBuild = tableMaker.make(random, step);
            final ColumnSource<?>[] sources = sources(toBuild);
            hasher.build(toBuild.getRowSet(), sources, (rows, ids, statuses) -> {
                if (incremental && ((IncrementalKeyIdHasherTypedBase) hasher).rehashPointer > 0) {
                    sawPartialRehash.setTrue();
                }
                final int[] position = {0};
                rows.forAllRowKeys(rowKey -> {
                    final List<Object> key = keyOf(sources, rowKey);
                    final byte status = statuses.get(position[0]);
                    final int id = ids.get(position[0]++);
                    assertNotEquals(KeyIdHasher.NULL_ID, id);
                    final Integer existing = expected.putIfAbsent(key, id);
                    if (existing != null) {
                        assertEquals((int) existing, id);
                        assertEquals(KeyIdHasher.FOUND, status);
                    } else {
                        assertEquals(KeyIdHasher.ADDED, status);
                    }
                });
            });
            assertEquals(expected.size(), hasher.size());
            maxLive = Math.max(maxLive, expected.size());
            // removed ids are reused before new ones are handed out
            assertTrue(hasher.idCapacity() <= maxLive);
            assertEquals(expected.size(), new IntOpenHashSet(expected.values()).size());

            final List<List<Object>> removed = new ArrayList<>();
            if (incremental) {
                final IncrementalKeyIdHasherTypedBase incrementalHasher = (IncrementalKeyIdHasherTypedBase) hasher;
                // remove a random subset of the keys
                final IntArrayList removedIds = new IntArrayList();
                expected.entrySet().removeIf(entry -> {
                    if (random.nextInt(3) == 0) {
                        removedIds.add((int) entry.getValue());
                        removed.add(entry.getKey());
                        return true;
                    }
                    return false;
                });
                incrementalHasher.remove(IntChunk.chunkWrap(removedIds.toIntArray()));
                assertEquals(expected.size(), hasher.size());
            }

            // every key that was built or removed probes to its id, or to NULL_ID once removed
            hasher.probe(toBuild.getRowSet(), sources, false, (rows, ids, statuses) -> {
                final int[] position = {0};
                rows.forAllRowKeys(rowKey -> {
                    final Integer id = expected.get(keyOf(sources, rowKey));
                    assertEquals(id == null ? KeyIdHasher.MISSING : KeyIdHasher.FOUND, statuses.get(position[0]));
                    assertEquals(id == null ? KeyIdHasher.NULL_ID : (int) id, ids.get(position[0]++));
                });
            });
            if (!removed.isEmpty()) {
                final Table removedKeys = keysTable(prototype, removed);
                hasher.probe(removedKeys.getRowSet(), sources(removedKeys), false, (rows, ids, statuses) -> {
                    for (int ii = 0; ii < ids.size(); ++ii) {
                        assertEquals(KeyIdHasher.NULL_ID, ids.get(ii));
                        assertEquals(KeyIdHasher.MISSING, statuses.get(ii));
                    }
                });
            }
        }
        return sawPartialRehash.booleanValue();
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

    private static Table keysTable(final Table prototype, final List<List<Object>> keys) {
        final String[] names = prototype.getDefinition().getColumnNamesArray();
        if (names.length == 1) {
            return newTable(longCol(names[0], keys.stream().mapToLong(key -> (Long) key.get(0)).toArray()));
        }
        return newTable(intCol(names[0], keys.stream().mapToInt(key -> (Integer) key.get(0)).toArray()),
                stringCol(names[1], keys.stream().map(key -> (String) key.get(1)).toArray(String[]::new)));
    }
}
