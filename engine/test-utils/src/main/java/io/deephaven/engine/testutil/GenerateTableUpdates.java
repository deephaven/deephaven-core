//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.sources.TestColumnSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.mutable.MutableLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.function.BiConsumer;

public class GenerateTableUpdates {

    public static void generateTableUpdates(int size, Random random, QueryTable table,
            ColumnInfo<?, ?>[] columnInfo) {
        final RowSet[] result = computeTableUpdates(size, random, table, columnInfo);
        table.notifyListeners(result[0], result[1], result[2]);
    }

    public static void generateAppends(final int size, Random random, QueryTable table,
            ColumnInfo<?, ?>[] columnInfos) {
        final long firstKey = table.getRowSet().lastRowKey() + 1;
        final int randomSize = 1 + random.nextInt(size);
        final RowSet keysToAdd = RowSetFactory.fromRange(firstKey, firstKey + randomSize - 1);
        final ColumnHolder<?>[] columnAdditions = new ColumnHolder[columnInfos.length];
        for (int i = 0; i < columnAdditions.length; i++) {
            columnAdditions[i] = columnInfos[i].generateUpdateColumnHolder(keysToAdd, random);
        }
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println();
        }
        TstUtils.addToTable(table, keysToAdd, columnAdditions);
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Add: " + keysToAdd);
            try {
                System.out.println("Updated Table:" + table.size());
                TableTools.showWithRowSet(table, 100);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        table.notifyListeners(keysToAdd, RowSetFactory.empty(), RowSetFactory.empty());
    }

    public static RowSet[] computeTableUpdates(int size, Random random, QueryTable table,
            ColumnInfo<?, ?>[] columnInfo) {
        return computeTableUpdates(size, random, table, columnInfo, true, true, true);
    }

    public static RowSet[] computeTableUpdates(int size, Random random, QueryTable table,
            ColumnInfo<?, ?>[] columnInfo, boolean add, boolean remove, boolean modify) {
        final RowSet keysToRemove;
        if (remove && table.getRowSet().size() > 0) {
            keysToRemove =
                    TstUtils.selectSubIndexSet(random.nextInt(table.getRowSet().intSize() + 1), table.getRowSet(),
                            random);
        } else {
            keysToRemove = TstUtils.i();
        }

        final RowSet keysToAdd =
                add ? TstUtils.newIndex(random.nextInt(size / 2 + 1), table.getRowSet(), random) : TstUtils.i();
        TstUtils.removeRows(table, keysToRemove);
        for (final ColumnInfo<?, ?> info : columnInfo) {
            info.remove(keysToRemove);
        }

        final RowSet keysToModify;
        if (modify && table.getRowSet().size() > 0) {
            keysToModify =
                    TstUtils.selectSubIndexSet(random.nextInt((int) table.getRowSet().size()), table.getRowSet(),
                            random);
        } else {
            keysToModify = TstUtils.i();
        }
        final ColumnHolder<?>[] columnAdditions = new ColumnHolder[columnInfo.length];
        for (int i = 0; i < columnAdditions.length; i++) {
            columnAdditions[i] = columnInfo[i].generateUpdateColumnHolder(keysToModify, random);
        }
        TstUtils.addToTable(table, keysToModify, columnAdditions);
        for (int i = 0; i < columnAdditions.length; i++) {
            columnAdditions[i] = columnInfo[i].generateUpdateColumnHolder(keysToAdd, random);
        }
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println();
        }
        TstUtils.addToTable(table, keysToAdd, columnAdditions);
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Add: " + keysToAdd);
            System.out.println("Remove: " + keysToRemove);
            System.out.println("Modify: " + keysToModify);
            try {
                System.out.println("Updated Table: " + table.size());
                TableTools.showWithRowSet(table, 100);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new RowSet[] {keysToAdd, keysToRemove, keysToModify};
    }

    public static class SimulationProfile {
        // Shift Strategy. Must sum to <= 100%.
        protected int SHIFT_10_PERCENT_KEY_SPACE = 10;
        protected int SHIFT_10_PERCENT_POS_SPACE = 30;
        protected int SHIFT_AGGRESSIVELY = 10;

        /** limit shift domain to less than 50% of keyspace */
        protected int SHIFT_LIMIT_50_PERCENT = 80;

        /** probability of modifying each column */
        protected int MOD_ADDITIONAL_COLUMN = 50;

        void validate() {
            validateGroup(SHIFT_10_PERCENT_KEY_SPACE, SHIFT_10_PERCENT_POS_SPACE, SHIFT_AGGRESSIVELY);
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

    public static final SimulationProfile DEFAULT_PROFILE = new SimulationProfile();

    public static final SimulationProfile NO_SHIFT_PROFILE =
            new SimulationProfile() {
                {
                    SHIFT_10_PERCENT_KEY_SPACE = 0;
                    SHIFT_10_PERCENT_POS_SPACE = 0;
                    SHIFT_AGGRESSIVELY = 0;
                }
            };

    public static void generateShiftAwareTableUpdates(final SimulationProfile profile, final int targetUpdateSize,
            final Random random, final QueryTable table,
            final ColumnInfo<?, ?>[] columnInfo) {
        profile.validate();

        try (final WritableRowSet rowSet = table.getRowSet().copy()) {
            final ColumnInfo<?, ?>[] mutableColumns =
                    Arrays.stream(columnInfo).filter(ci -> !ci.immutable).toArray(ColumnInfo[]::new);
            final boolean hasImmutableColumns = columnInfo.length > mutableColumns.length;

            final TableUpdateImpl update = new TableUpdateImpl();

            // Removes in pre-shift keyspace.
            if (rowSet.isNonempty()) {
                update.removed =
                        TstUtils.selectSubIndexSet(Math.min(rowSet.intSize(), random.nextInt(targetUpdateSize)),
                                rowSet, random);
                rowSet.remove(update.removed()); // remove blatted and explicit removals
            } else {
                update.removed = TstUtils.i();
            }

            // Generate Shifts.
            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            if (!hasImmutableColumns) {
                MutableLong lastDest = new MutableLong();

                BiConsumer<Long, Long> shiftConsumer = (first, last) -> {
                    if (first < 0 || last < 0 || last < first)
                        return;

                    final long len = last - first + 1;

                    final long minShift;
                    final long maxShift;
                    if (shiftBuilder.nonempty()) {
                        minShift = lastDest.get() + 1 - first;
                        maxShift = Math.max(minShift,
                                random.nextInt(100) < profile.SHIFT_LIMIT_50_PERCENT ? (len + 1) / 2 : 2 * len);
                    } else {
                        maxShift = random.nextInt(100) < profile.SHIFT_LIMIT_50_PERCENT ? (len + 1) / 2 : 2 * len;
                        minShift = -maxShift;
                    }

                    long shiftDelta = 0;
                    while (shiftDelta == 0) {
                        shiftDelta = Math.max(-first, minShift + nextLong(random, maxShift - minShift + 1));
                    }

                    lastDest.set(last + shiftDelta);
                    shiftBuilder.shiftRange(first, last, shiftDelta);
                };

                int shiftStrategy = random.nextInt(100);
                if (shiftStrategy < profile.SHIFT_10_PERCENT_KEY_SPACE && rowSet.isNonempty()) {
                    // 10% of keyspace
                    final long startKey = nextLong(random, rowSet.lastRowKey() + 1);
                    final long lastKey = Math.min(startKey + (long) (rowSet.lastRowKey() * 0.1), rowSet.lastRowKey());
                    shiftConsumer.accept(startKey, lastKey);
                }
                shiftStrategy -= profile.SHIFT_10_PERCENT_KEY_SPACE;

                if (shiftStrategy >= 0 && shiftStrategy < profile.SHIFT_10_PERCENT_POS_SPACE && rowSet.isNonempty()) {
                    // 10% of keys
                    final long startIdx = nextLong(random, rowSet.size());
                    final long lastIdx = Math.min(rowSet.size() - 1, startIdx + (rowSet.size() / 10));
                    shiftConsumer.accept(rowSet.get(startIdx), rowSet.get(lastIdx));
                }
                shiftStrategy -= profile.SHIFT_10_PERCENT_POS_SPACE;

                if (shiftStrategy >= 0 && shiftStrategy < profile.SHIFT_AGGRESSIVELY && rowSet.isNonempty()) {
                    // aggressive shifting
                    long currIdx = 0;
                    while (currIdx < rowSet.size()) {
                        final long startIdx = currIdx + (nextLong(random, rowSet.size() - currIdx));
                        final long lastIdx = startIdx + (long) (Math.sqrt(nextLong(random, rowSet.size() - startIdx)));
                        shiftConsumer.accept(rowSet.get(startIdx), rowSet.get(lastIdx));
                        currIdx = 1 + lastIdx + (long) (Math.sqrt(nextLong(random, rowSet.size() - lastIdx)));
                    }
                }
                shiftStrategy -= profile.SHIFT_AGGRESSIVELY;
            }
            update.shifted = shiftBuilder.build();

            // Compute what data needs to be removed otherwise the shift generated would be invalid. We must also update
            // our copied RowSet so that we can pick appropriate added and modified sets.
            final int preShiftIndexSize = rowSet.intSize();
            update.shifted().apply((start, end, delta) -> {
                // Remove any keys that are going to be splatted all over thanks to a shift.
                final long blatStart = delta < 0 ? start + delta : end;
                final long blatEnd = delta < 0 ? start - 1 : end + delta;
                try (final RowSet blattedRows =
                        rowSet.extract(RowSetFactory.fromRange(blatStart, blatEnd))) {
                    update.removed().writableCast().insert(blattedRows);
                }
            });
            final int numRowsBlattedByShift = preShiftIndexSize - rowSet.intSize();

            update.shifted().apply(rowSet);

            // Modifies and Adds in post-shift keyspace.
            if (rowSet.isNonempty()) {
                update.modified = TstUtils.selectSubIndexSet(
                        Math.min(rowSet.intSize(), random.nextInt(targetUpdateSize * 2)), rowSet, random);
            } else {
                update.modified = TstUtils.i();
            }

            if (update.modified().isEmpty()) {
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            } else {
                final ArrayList<String> modifiedColumns = new ArrayList<>();
                update.modifiedColumnSet = table.getModifiedColumnSetForUpdates();
                update.modifiedColumnSet.clear();

                final String mustModifyColumn = (mutableColumns.length == 0) ? null
                        : mutableColumns[random.nextInt(mutableColumns.length)].name;
                for (final ColumnInfo<?, ?> ci : columnInfo) {
                    if (ci.name.equals(mustModifyColumn)
                            || (!ci.immutable && random.nextInt(100) < profile.MOD_ADDITIONAL_COLUMN)) {
                        modifiedColumns.add(ci.name);
                    }
                }
                update.modifiedColumnSet().setAll(modifiedColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            }

            update.added = TstUtils.newIndex(numRowsBlattedByShift + random.nextInt(targetUpdateSize), rowSet, random);

            generateTableUpdates(update, random, table, columnInfo);
        }
    }

    public static void generateTableUpdates(final TableUpdate update,
            final Random random, final QueryTable table,
            final ColumnInfo<?, ?>[] columnInfo) {
        final WritableRowSet rowSet = table.getRowSet().writableCast();

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println();
            System.out.println("TrackingWritableRowSet: " + rowSet);
        }

        // Remove data:
        TstUtils.removeRows(table, update.removed());
        for (final ColumnInfo<?, ?> info : columnInfo) {
            info.remove(update.removed());
        }
        rowSet.remove(update.removed());

        // Shift data:
        update.shifted().apply((start, end, delta) -> {
            // Move data!
            for (final ColumnInfo<?, ?> info : columnInfo) {
                info.shift(start, end, delta);
            }
            for (final ColumnSource<?> column : table.getColumnSources()) {
                if (column instanceof TestColumnSource) {
                    final TestColumnSource<?> testSource = (TestColumnSource<?>) column;
                    testSource.shift(start, end, delta);
                }
            }
        });
        update.shifted().apply(rowSet);

        // Modifies and Adds in post-shift keyspace.
        final ColumnHolder<?>[] cModsOnly = new ColumnHolder[columnInfo.length];
        final ColumnHolder<?>[] cAddsOnly = new ColumnHolder[columnInfo.length];

        final BitSet dirtyColumns = update.modifiedColumnSet().extractAsBitSet();
        for (int i = 0; i < columnInfo.length; i++) {
            final ColumnInfo<?, ?> ci = columnInfo[i];
            final RowSet keys = dirtyColumns.get(i) ? update.modified() : TstUtils.i();
            cModsOnly[i] = ci.generateUpdateColumnHolder(keys, random);
            cAddsOnly[i] = ci.generateUpdateColumnHolder(update.added(), random);
        }
        TstUtils.addToTable(table, update.added(), cAddsOnly);
        TstUtils.addToTable(table, update.modified(), cModsOnly);
        rowSet.insert(update.added());

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Add: " + update.added());
            System.out.println("Remove: " + update.removed());
            System.out.println("Modify: " + update.modified());
            System.out.println("Shift: " + update.shifted());
            System.out.println("ModifiedColumnSet: " + update.modifiedColumnSet());
            try {
                System.out.println("Updated Table: " + table.size());
                TableTools.showWithRowSet(table, 100);
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
