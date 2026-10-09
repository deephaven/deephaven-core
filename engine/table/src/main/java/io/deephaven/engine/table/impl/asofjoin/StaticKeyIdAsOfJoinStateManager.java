//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.join.StaticKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import org.jetbrains.annotations.NotNull;

/**
 * The as-of join state manager when the right table is static, and the left table is static or refreshing.
 * <p>
 * Each distinct key has an id from a {@link StaticKeyIdHasherTypedBase}, which is the key's slot: the left rows and the
 * right rows of each key are kept by id. A build or probe collects each side's rows in a sequential builder; the right
 * builders become row sets before the right stamps are read, and the left builder of a slot is consumed and cleared
 * each time its row set is taken, so that a refreshing left table collects each update's rows afresh.
 */
public class StaticKeyIdAsOfJoinStateManager extends StaticHashedAsOfJoinStateManager {

    /**
     * The right state of a slot whose right row set has been released; the slot stays in the table for probing.
     */
    private static final Object RELEASED_RIGHT_STATE = new Object();

    private final StaticKeyIdHasherTypedBase hasher;

    // the builder of each key's left rows since its row set was last taken, by id; null for none
    private final ObjectArraySource<RowSetBuilderSequential> leftRowSet =
            new ObjectArraySource<>(RowSetBuilderSequential.class);

    // the right rows of each key, by id: a builder until the right builders are converted, then a row set, null for
    // none, or RELEASED_RIGHT_STATE
    private final ObjectArraySource<Object> rightRowSet = new ObjectArraySource<>(Object.class);

    /**
     * Whether the right builders have been converted to row sets, which must happen before a right row set is read.
     */
    private boolean rightBuildersConverted = false;

    /**
     * Whether the right row sets were built here and are closed on release, rather than supplied by a data index that
     * owns them.
     */
    private boolean rightRowSetsOwned;

    /**
     * @param tableKeySources the key sources the hash table is built from
     * @param keySourcesForErrorMessages the key sources that error messages render keys from
     * @param tableSize the initial number of hash table slots, a power of two
     * @param maximumLoadFactor the fraction of the slots that may be occupied before the table grows
     */
    public StaticKeyIdAsOfJoinStateManager(
            final ColumnSource<?>[] tableKeySources,
            final ColumnSource<?>[] keySourcesForErrorMessages,
            final int tableSize,
            final double maximumLoadFactor) {
        super(keySourcesForErrorMessages);
        hasher = StaticKeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
    }

    private void ensureIdCapacity() {
        final int capacity = hasher.idCapacity();
        leftRowSet.ensureCapacity(capacity);
        rightRowSet.ensureCapacity(capacity);
    }

    /**
     * Add a left row key to a slot.
     *
     * @return true if this is the slot's first left row key since its left row set was last taken
     */
    private boolean addLeftKey(final int slot, final long rowKey) {
        RowSetBuilderSequential builder = leftRowSet.getUnsafe(slot);
        final boolean first = builder == null;
        if (first) {
            leftRowSet.set(slot, builder = RowSetFactory.builderSequential());
        }
        builder.appendKey(rowKey);
        return first;
    }

    private void addRightKey(final int slot, final long rowKey) {
        RowSetBuilderSequential builder = (RowSetBuilderSequential) rightRowSet.getUnsafe(slot);
        if (builder == null) {
            rightRowSet.set(slot, builder = RowSetFactory.builderSequential());
        }
        builder.appendKey(rowKey);
    }

    /**
     * Report every slot with right state, which after a build is every slot.
     */
    private int fillSlots(@NotNull final IntegerArraySource slots) {
        final int capacity = hasher.idCapacity();
        slots.ensureCapacity(capacity);
        int slotCount = 0;
        for (int slot = 0; slot < capacity; ++slot) {
            if (rightRowSet.getUnsafe(slot) != null) {
                slots.set(slotCount++, slot);
            }
        }
        return slotCount;
    }

    @Override
    public int buildFromLeftSide(final RowSequence leftRows, final ColumnSource<?>[] leftSources,
            @NotNull final IntegerArraySource addedSlots) {
        hasher.build(leftRows, leftSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int slot = ids.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    // every left key has right state, so that it is reported with the added slots
                    rightRowSet.set(slot, RowSetFactory.builderSequential());
                }
                addLeftKey(slot, rowKeys.get(ii));
            }
        });
        hasher.complete();
        return fillSlots(addedSlots);
    }

    @Override
    public int buildFromRightSide(final RowSequence rightRows, final ColumnSource<?>[] rightSources,
            @NotNull final IntegerArraySource addedSlots) {
        hasher.build(rightRows, rightSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                addRightKey(ids.get(ii), rowKeys.get(ii));
            }
        });
        hasher.complete();
        return fillSlots(addedSlots);
    }

    @Override
    public void probeLeft(final RowSequence leftRows, final ColumnSource<?>[] leftSources) {
        hasher.probe(leftRows, leftSources, false, (rows, ids) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int slot = ids.get(ii);
                if (slot != KeyIdHasher.NULL_ID) {
                    addLeftKey(slot, rowKeys.get(ii));
                }
            }
        });
    }

    @Override
    public int probeLeft(final RowSequence leftRows, final ColumnSource<?>[] leftSources,
            @NotNull final IntegerArraySource slots) {
        final int[] slotCount = {0};
        hasher.probe(leftRows, leftSources, false, (rows, ids) -> {
            // each probed row reports at most one new slot, and there are at most idCapacity slots
            slots.ensureCapacity(Math.min((long) slotCount[0] + ids.size(), hasher.idCapacity()));
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            slotCount[0] = probeLeftChunk(rowKeys, ids, slots, slotCount[0]);
        });
        return slotCount[0];
    }

    private int probeLeftChunk(final LongChunk<OrderedRowKeys> rowKeys, final IntChunk<Values> ids,
            final IntegerArraySource slots, int slotCount) {
        final int size = ids.size();
        for (int ii = 0; ii < size; ++ii) {
            final int slot = ids.get(ii);
            if (slot != KeyIdHasher.NULL_ID && addLeftKey(slot, rowKeys.get(ii))) {
                slots.set(slotCount++, slot);
            }
        }
        return slotCount;
    }

    @Override
    public void probeRight(final RowSequence rightRows, final ColumnSource<?>[] rightSources) {
        hasher.probe(rightRows, rightSources, false, (rows, ids) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int slot = ids.get(ii);
                if (slot != KeyIdHasher.NULL_ID) {
                    addRightKey(slot, rowKeys.get(ii));
                }
            }
        });
    }

    @Override
    public int getSlotCapacity() {
        return hasher.idCapacity();
    }

    /**
     * Take the left rows collected for a slot since its row set was last taken, clearing them; so a refreshing left
     * table collects each update's rows afresh.
     *
     * @param slot the slot
     * @return the slot's left row set, or null if no left row was collected
     */
    @Override
    public RowSet getLeftRowSet(final int slot) {
        final RowSetBuilderSequential builder = leftRowSet.getAndSetUnsafe(slot, null);
        return builder == null ? null : builder.build();
    }

    @Override
    public RowSet getRightRowset(final int slot) {
        if (!rightBuildersConverted) {
            throw new IllegalStateException(
                    "getRightRowset() may not be called before convertRightBuildersToRowSet() or populateRightRowSetsFromIndexTable()");
        }
        final Object rightState = rightRowSet.getUnsafe(slot);
        return rightState == RELEASED_RIGHT_STATE ? null : (RowSet) rightState;
    }

    @Override
    public void releaseRightRowSet(final int slot) {
        final Object rightState = rightRowSet.getUnsafe(slot);
        if (rightState instanceof RowSet) {
            if (rightRowSetsOwned) {
                ((RowSet) rightState).close();
            }
            rightRowSet.set(slot, RELEASED_RIGHT_STATE);
        }
    }

    @Override
    public void convertRightBuildersToRowSet(@NotNull final IntegerArraySource slots, final int slotCount) {
        Assert.eqFalse(rightBuildersConverted, "rightBuildersConverted");
        rightBuildersConverted = true;
        rightRowSetsOwned = true;

        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final int slot = slots.getInt(slotIndex);
            final RowSetBuilderSequential builder = (RowSetBuilderSequential) rightRowSet.getUnsafe(slot);
            if (builder == null) {
                continue;
            }
            final WritableRowSet rowSet = builder.build();
            if (rowSet.isEmpty()) {
                rightRowSet.set(slot, null);
                rowSet.close();
            } else {
                rightRowSet.set(slot, rowSet);
            }
        }
    }

    @Override
    public void populateRightRowSetsFromIndexTable(
            @NotNull final IntegerArraySource slots,
            final int slotCount,
            @NotNull final ColumnSource<RowSet> rowSetSource) {
        Assert.eqFalse(rightBuildersConverted, "rightBuildersConverted");
        rightBuildersConverted = true;

        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final int slot = slots.getInt(slotIndex);
            final RowSetBuilderSequential builder = (RowSetBuilderSequential) rightRowSet.getUnsafe(slot);
            if (builder == null) {
                continue;
            }
            try (final RowSet indexRows = builder.build()) {
                if (indexRows.isEmpty()) {
                    rightRowSet.set(slot, null);
                } else if (indexRows.size() == 1) {
                    // the right table is static, so its index row sets never change; they belong to the index
                    rightRowSet.set(slot, rowSetSource.get(indexRows.firstRowKey()));
                } else {
                    throw new IllegalStateException("Index-built row set should have exactly one value: " + indexRows);
                }
            }
        }
    }
}
