/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.DateTimeTreeMapSource;
import io.deephaven.db.v2.sources.TreeMapSource;
import io.deephaven.db.v2.utils.ColumnHolder;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.function.BiConsumer;

public class GenerateTableUpdates {

    static public void generateTableUpdates(int size, Random random, QueryTable table,
        TstUtils.ColumnInfo[] columnInfo) {
        final Index[] result = computeTableUpdates(size, random, table, columnInfo);
        table.notifyListeners(result[0], result[1], result[2]);
    }

    public static void generateAppends(final int size, Random random, QueryTable table,
        TstUtils.ColumnInfo[] columnInfos) {
        final long firstKey = table.getIndex().lastKey() + 1;
        final int randomSize = 1 + random.nextInt(size);
        final Index keysToAdd = Index.FACTORY.getIndexByRange(firstKey, firstKey + randomSize - 1);
        final ColumnHolder[] columnAdditions = new ColumnHolder[columnInfos.length];
        for (int i = 0; i < columnAdditions.length; i++) {
            columnAdditions[i] = columnInfos[i].populateMapAndC(keysToAdd, random);
        }
        if (LiveTableTestCase.printTableUpdates) {
            System.out.println();
        }
        TstUtils.addToTable(table, keysToAdd, columnAdditions);
        if (LiveTableTestCase.printTableUpdates) {
            System.out.println("Add: " + keysToAdd);
            try {
                System.out.println("Updated Table:" + table.size());
                TableTools.showWithIndex(table, 100);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        table.notifyListeners(keysToAdd, Index.FACTORY.getEmptyIndex(),
            Index.FACTORY.getEmptyIndex());
    }

    static public Index[] computeTableUpdates(int size, Random random, QueryTable table,
        TstUtils.ColumnInfo[] columnInfo) {
        return computeTableUpdates(size, random, table, columnInfo, true, true, true);
    }

    static public Index[] computeTableUpdates(int size, Random random, QueryTable table,
        TstUtils.ColumnInfo[] columnInfo, boolean add, boolean remove, boolean modify) {
        final Index keysToRemove;
        if (remove && table.getIndex().size() > 0) {
            keysToRemove = TstUtils.selectSubIndexSet(
                random.nextInt(table.getIndex().intSize() + 1), table.getIndex(), random);
        } else {
            keysToRemove = TstUtils.i();
        }

        final Index keysToAdd =
            add ? TstUtils.newIndex(random.nextInt(size / 2 + 1), table.getIndex(), random)
                : TstUtils.i();
        TstUtils.removeRows(table, keysToRemove);
        for (final Index.Iterator iterator = keysToRemove.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            for (final TstUtils.ColumnInfo info : columnInfo) {
                info.remove(next);
            }
        }

        final Index keysToModify;
        if (modify && table.getIndex().size() > 0) {
            keysToModify = TstUtils.selectSubIndexSet(random.nextInt((int) table.getIndex().size()),
                table.getIndex(), random);
        } else {
            keysToModify = TstUtils.i();
        }
        final ColumnHolder columnAdditions[] = new ColumnHolder[columnInfo.length];
        for (int i = 0; i < columnAdditions.length; i++) {
            columnAdditions[i] = columnInfo[i].populateMapAndC(keysToModify, random);
        }
        TstUtils.addToTable(table, keysToModify, columnAdditions);
        for (int i = 0; i < columnAdditions.length; i++) {
            columnAdditions[i] = columnInfo[i].populateMapAndC(keysToAdd, random);
        }
        if (LiveTableTestCase.printTableUpdates) {
            System.out.println();
        }
        TstUtils.addToTable(table, keysToAdd, columnAdditions);
        if (LiveTableTestCase.printTableUpdates) {
            System.out.println("Add: " + keysToAdd);
            System.out.println("Remove: " + keysToRemove);
            System.out.println("Modify: " + keysToModify);
            try {
                System.out.println("Updated Table: " + table.size());
                TableTools.showWithIndex(table, 100);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new Index[] {keysToAdd, keysToRemove, keysToModify};
    }

    public static class SimulationProfile {
        // Shift Strategy. Must sum to <= 100%.
        int SHIFT_10_PERCENT_KEY_SPACE = 10;
        int SHIFT_10_PERCENT_POS_SPACE = 30;
        int SHIFT_AGGRESSIVELY = 10;

        int SHIFT_LIMIT_50_PERCENT = 80; // limit shift domain to less than 50% of keyspace

        int MOD_ADDITIONAL_COLUMN = 50; // probability of modifying each column

        void validate() {
            validateGroup(SHIFT_10_PERCENT_KEY_SPACE, SHIFT_10_PERCENT_POS_SPACE,
                SHIFT_AGGRESSIVELY);
            validateGroup(SHIFT_LIMIT_50_PERCENT);
            validateGroup(MOD_ADDITIONAL_COLUMN);
        }

        private void validateGroup(int... opts) {
            int sum = 0;
            for (int opt : opts) {
                sum += opt;
                Assert.geqZero(opt, "Simulation Profile Percentage");
            }
            Assert.leq(sum, "Simulation Profile Group Percentage", 100, "100%");
        }
    }

    static public final SimulationProfile DEFAULT_PROFILE = new SimulationProfile();

    static public void generateShiftAwareTableUpdates(final SimulationProfile profile,
        final int targetUpdateSize,
        final Random random, final QueryTable table,
        final TstUtils.ColumnInfo<?, ?>[] columnInfo) {
        profile.validate();

        try (final Index index = table.getIndex().clone()) {
            final TstUtils.ColumnInfo<?, ?>[] mutableColumns = Arrays.stream(columnInfo)
                .filter(ci -> !ci.immutable).toArray(TstUtils.ColumnInfo[]::new);
            final boolean hasImmutableColumns = columnInfo.length > mutableColumns.length;

            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();

            // Removes in pre-shift keyspace.
            if (index.size() > 0) {
                update.removed = TstUtils.selectSubIndexSet(
                    Math.min(index.intSize(), random.nextInt(targetUpdateSize)), index, random);
                index.remove(update.removed); // remove blatted and explicit removals
            } else {
                update.removed = TstUtils.i();
            }

            // Generate Shifts.
            final IndexShiftData.Builder shiftBuilder = new IndexShiftData.Builder();
            if (!hasImmutableColumns) {
                MutableLong lastDest = new MutableLong();

                BiConsumer<Long, Long> shiftConsumer = (first, last) -> {
                    if (first < 0 || last < 0 || last < first)
                        return;

                    final long len = last - first + 1;

                    final long minShift;
                    final long maxShift;
                    if (shiftBuilder.nonempty()) {
                        minShift = lastDest.longValue() + 1 - first;
                        maxShift = Math.max(minShift,
                            random.nextInt(100) < profile.SHIFT_LIMIT_50_PERCENT ? (len + 1) / 2
                                : 2 * len);
                    } else {
                        maxShift =
                            random.nextInt(100) < profile.SHIFT_LIMIT_50_PERCENT ? (len + 1) / 2
                                : 2 * len;
                        minShift = -maxShift;
                    }

                    long shiftDelta = 0;
                    while (shiftDelta == 0) {
                        shiftDelta =
                            Math.max(-first, minShift + nextLong(random, maxShift - minShift + 1));
                    }

                    lastDest.setValue(last + shiftDelta);
                    shiftBuilder.shiftRange(first, last, shiftDelta);
                };

                int shiftStrategy = random.nextInt(100);
                if (shiftStrategy < profile.SHIFT_10_PERCENT_KEY_SPACE && index.nonempty()) {
                    // 10% of keyspace
                    final long startKey = nextLong(random, index.lastKey() + 1);
                    final long lastKey =
                        Math.min(startKey + (long) (index.lastKey() * 0.1), index.lastKey());
                    shiftConsumer.accept(startKey, lastKey);
                }
                shiftStrategy -= profile.SHIFT_10_PERCENT_KEY_SPACE;

                if (shiftStrategy >= 0 && shiftStrategy < profile.SHIFT_10_PERCENT_POS_SPACE
                    && index.nonempty()) {
                    // 10% of keys
                    final long startIdx = nextLong(random, index.size());
                    final long lastIdx = Math.min(index.size() - 1, startIdx + (index.size() / 10));
                    shiftConsumer.accept(index.get(startIdx), index.get(lastIdx));
                }
                shiftStrategy -= profile.SHIFT_10_PERCENT_POS_SPACE;

                if (shiftStrategy >= 0 && shiftStrategy < profile.SHIFT_AGGRESSIVELY
                    && index.nonempty()) {
                    // aggressive shifting
                    long currIdx = 0;
                    while (currIdx < index.size()) {
                        final long startIdx = currIdx + (nextLong(random, index.size() - currIdx));
                        final long lastIdx = startIdx
                            + (long) (Math.sqrt(nextLong(random, index.size() - startIdx)));
                        shiftConsumer.accept(index.get(startIdx), index.get(lastIdx));
                        currIdx = 1 + lastIdx
                            + (long) (Math.sqrt(nextLong(random, index.size() - lastIdx)));
                    }
                }
                shiftStrategy -= profile.SHIFT_AGGRESSIVELY;
            }
            update.shifted = shiftBuilder.build();

            // Compute what data needs to be removed otherwise the shift generated would be invalid.
            // We must also update
            // our cloned index so we can pick appropriate added and modified sets.
            final int preShiftIndexSize = index.intSize();
            update.shifted.apply((start, end, delta) -> {
                // Remove any keys that are going to be splatted all over thanks to a shift.
                final long blatStart = delta < 0 ? start + delta : end;
                final long blatEnd = delta < 0 ? start - 1 : end + delta;
                try (final Index blattedRows =
                    index.extract(Index.CURRENT_FACTORY.getIndexByRange(blatStart, blatEnd))) {
                    update.removed.insert(blattedRows);
                }
            });
            final int numRowsBlattedByShift = preShiftIndexSize - index.intSize();

            update.shifted.apply(index);

            // Modifies and Adds in post-shift keyspace.
            if (index.nonempty()) {
                update.modified = TstUtils.selectSubIndexSet(
                    Math.min(index.intSize(), random.nextInt(targetUpdateSize * 2)), index, random);
            } else {
                update.modified = TstUtils.i();
            }

            if (update.modified.empty()) {
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                final ArrayList<String> modifiedColumns = new ArrayList<>();
                update.modifiedColumnSet = table.modifiedColumnSet;
                update.modifiedColumnSet.clear();

                final String mustModifyColumn = (mutableColumns.length == 0) ? null
                    : mutableColumns[random.nextInt(mutableColumns.length)].name;
                for (final TstUtils.ColumnInfo<?, ?> ci : columnInfo) {
                    if (ci.name.equals(mustModifyColumn)
                        || (!ci.immutable && random.nextInt(100) < profile.MOD_ADDITIONAL_COLUMN)) {
                        modifiedColumns.add(ci.name);
                    }
                }
                update.modifiedColumnSet
                    .setAll(modifiedColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            }

            update.added = TstUtils
                .newIndex(numRowsBlattedByShift + random.nextInt(targetUpdateSize), index, random);

            generateTableUpdates(update, random, table, columnInfo);
        }
    }

    static public void generateTableUpdates(final ShiftAwareListener.Update update,
        final Random random, final QueryTable table,
        final TstUtils.ColumnInfo<?, ?>[] columnInfo) {
        final Index index = table.getIndex();

        if (LiveTableTestCase.printTableUpdates) {
            System.out.println();
            System.out.println("Index: " + index);
        }

        // Remove data:
        TstUtils.removeRows(table, update.removed);
        for (final Index.Iterator iterator = update.removed.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            for (final TstUtils.ColumnInfo<?, ?> info : columnInfo) {
                info.remove(next);
            }
        }
        index.remove(update.removed);

        // Shift data:
        update.shifted.apply((start, end, delta) -> {
            // Move data!
            final Index.SearchIterator iter =
                (delta < 0) ? index.searchIterator() : index.reverseIterator();
            if (iter.advance((delta < 0) ? start : end)) {
                long idx = iter.currentValue();
                do {
                    if (idx < start || idx > end) {
                        break;
                    }
                    for (final TstUtils.ColumnInfo<?, ?> info : columnInfo) {
                        info.move(idx, idx + delta);
                    }
                    idx = iter.hasNext() ? iter.nextLong() : Index.NULL_KEY;
                } while (idx != Index.NULL_KEY);
            }
            for (final ColumnSource<?> column : table.getColumnSources()) {
                if (column instanceof TreeMapSource) {
                    final TreeMapSource<?> treeMapSource = (TreeMapSource<?>) column;
                    treeMapSource.shift(start, end, delta);
                } else if (column instanceof DateTimeTreeMapSource) {
                    final DateTimeTreeMapSource treeMapSource = (DateTimeTreeMapSource) column;
                    treeMapSource.shift(start, end, delta);
                }
            }
        });
        update.shifted.apply(index);

        // Modifies and Adds in post-shift keyspace.
        final ColumnHolder[] cModsOnly = new ColumnHolder[columnInfo.length];
        final ColumnHolder[] cAddsOnly = new ColumnHolder[columnInfo.length];

        final BitSet dirtyColumns = update.modifiedColumnSet.extractAsBitSet();
        for (int i = 0; i < columnInfo.length; i++) {
            final TstUtils.ColumnInfo<?, ?> ci = columnInfo[i];
            final Index keys = dirtyColumns.get(i) ? update.modified : TstUtils.i();
            cModsOnly[i] = ci.populateMapAndC(keys, random);
            cAddsOnly[i] = ci.populateMapAndC(update.added, random);
        }
        TstUtils.addToTable(table, update.added, cAddsOnly);
        TstUtils.addToTable(table, update.modified, cModsOnly);
        index.insert(update.added);

        if (LiveTableTestCase.printTableUpdates) {
            System.out.println("Add: " + update.added);
            System.out.println("Remove: " + update.removed);
            System.out.println("Modify: " + update.modified);
            System.out.println("Shift: " + update.shifted);
            System.out.println("ModifiedColumnSet: " + update.modifiedColumnSet);
            try {
                System.out.println("Updated Table: " + table.size());
                TableTools.showWithIndex(table, 100);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        table.notifyListeners(update);
    }

    static private long nextLong(final Random random, long bound) {
        while (true) {
            final long next = Math.abs(random.nextLong());
            if (next < 0) {
                continue;
            }
            return next % bound;
        }
    }
}
