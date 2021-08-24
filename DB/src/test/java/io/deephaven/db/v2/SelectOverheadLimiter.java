package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.sources.SwitchColumnSource;
import io.deephaven.db.v2.sources.sparse.SparseConstants;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.annotations.VisibleForTesting;
import gnu.trove.map.hash.TLongIntHashMap;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The table {@link Table#select} or {@link Table#update} and operations produce sparse sources as
 * of Treasure. If you have a sparse index, that means that you can have many blocks which only
 * actually contain one or very few elements. The {@link #clampSelectOverhead(Table, double)} method
 * is intended to precede a select or update operation, to limit the amount of memory overhead
 * allowed. For tables that are relatively dense, the original indices are preserved. If the
 * overhead exceeds the allowable factor, then the table is flattened before passing updates to
 * select. Once a table is made flat, it will not revert to it's original address space but rather
 * remain flat.
 */
public class SelectOverheadLimiter {
    @VisibleForTesting
    static final AtomicInteger conversions = new AtomicInteger(0);

    private SelectOverheadLimiter() {};

    private static class OverheadTracker implements IndexShiftData.SingleElementShiftCallback {
        TLongIntHashMap blockReferences = new TLongIntHashMap();
        long size;

        void addIndex(Index index) {
            size += index.size();
            index.forAllLongs(key -> {
                final long block = key >> SparseConstants.LOG_BLOCK_SIZE;
                blockReferences.adjustOrPutValue(block, 1, 1);
            });
        }

        void removeIndex(Index index) {
            size -= index.size();
            index.forAllLongs(key -> {
                final long block = key >> SparseConstants.LOG_BLOCK_SIZE;
                final long newReferences = blockReferences.adjustOrPutValue(block, -1, -1);
                Assert.geqZero(newReferences, "newReferences");
                if (newReferences == 0) {
                    blockReferences.remove(block);
                }
            });
        }

        private long blockCount() {
            return blockReferences.size();
        }

        private long size() {
            return size;
        }

        double overhead() {
            final long minimumBlocks =
                (size() + SparseConstants.BLOCK_SIZE - 1) / SparseConstants.BLOCK_SIZE;
            return (double) blockCount() / (double) minimumBlocks;
        }

        void clear() {
            blockReferences.clear();
        }

        @Override
        public void shift(long key, long shiftDelta) {
            final long oldBlock = key >> SparseConstants.LOG_BLOCK_SIZE;
            final long newBlock = (key + shiftDelta) >> SparseConstants.LOG_BLOCK_SIZE;
            if (oldBlock != newBlock) {
                final long oldReferences = blockReferences.adjustOrPutValue(oldBlock, -1, -1);
                Assert.geqZero(oldReferences, "newReferences");
                if (oldReferences == 0) {
                    blockReferences.remove(oldBlock);
                }
                blockReferences.adjustOrPutValue(newBlock, 1, 1);
            }
        }
    }

    public static Table clampSelectOverhead(Table input, double permittedOverhead) {
        if (!input.isLive()) {
            return input.flatten();
        }

        LiveTableMonitor.DEFAULT.checkInitiateTableOperation();

        // now we know we are refreshing, so should update our overhead structure
        final OverheadTracker overheadTracker = new OverheadTracker();
        overheadTracker.addIndex(input.getIndex());
        if (overheadTracker.overhead() > permittedOverhead) {
            return input.flatten();
        }

        // we are refreshing, and within the permitted overhead

        final Index index = input.getIndex().clone();
        final Map<String, SwitchColumnSource> resultColumns = new LinkedHashMap<>();
        // noinspection unchecked
        input.getColumnSourceMap()
            .forEach((name, cs) -> resultColumns.put(name, new SwitchColumnSource(cs)));
        final QueryTable result = new QueryTable(index, resultColumns);



        final MutableObject<ListenerRecorder> inputRecorder = new MutableObject<>(
            new ListenerRecorder("clampSelectOverhead.input()", (DynamicTable) input, result));
        ((DynamicTable) input).listenForUpdates(inputRecorder.getValue());
        final List<ListenerRecorder> recorders = Collections.synchronizedList(new ArrayList<>());
        recorders.add(inputRecorder.getValue());

        final MergedListener mergedListener = new MergedListener(recorders,
            Collections.singletonList((NotificationQueue.Dependency) input), "clampSelectOverhead",
            result) {
            Table flatResult = null;
            ListenerRecorder flatRecorder;
            ModifiedColumnSet.Transformer flatTransformer;
            ModifiedColumnSet.Transformer inputTransformer;

            {
                inputRecorder.getValue().setMergedListener(this);
                inputTransformer = ((BaseTable) input).newModifiedColumnSetTransformer(result,
                    result.getColumnSourceMap().keySet()
                        .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            }

            @Override
            protected void process() {
                if (flatResult != null) {
                    final ShiftAwareListener.Update upstream = flatRecorder.getUpdate();
                    index.remove(upstream.removed);
                    upstream.shifted.apply(index);
                    index.insert(upstream.added);
                    final ShiftAwareListener.Update copy = upstream.copy();
                    copy.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                    flatTransformer.clearAndTransform(upstream.modifiedColumnSet,
                        copy.modifiedColumnSet);
                    result.notifyListeners(copy);
                    return;
                }

                final ShiftAwareListener.Update upstream = inputRecorder.getValue().getUpdate();
                overheadTracker.removeIndex(upstream.removed);
                index.remove(upstream.removed);
                upstream.shifted.forAllInIndex(index, overheadTracker);
                upstream.shifted.apply(index);
                overheadTracker.addIndex(upstream.added);
                index.insert(upstream.added);

                if (overheadTracker.overhead() <= permittedOverhead) {
                    final ShiftAwareListener.Update copy = upstream.copy();
                    copy.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                    inputTransformer.clearAndTransform(upstream.modifiedColumnSet,
                        copy.modifiedColumnSet);
                    result.notifyListeners(copy);
                    return;
                }

                // we need to convert this to the flat table
                overheadTracker.clear();
                flatResult = input.flatten();
                flatRecorder = new ListenerRecorder("clampSelectOverhead.flatResult()",
                    (DynamicTable) flatResult, result);
                flatRecorder.setMergedListener(this);
                flatTransformer = ((BaseTable) flatResult).newModifiedColumnSetTransformer(result,
                    result.getColumnSourceMap().keySet()
                        .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

                ((DynamicTable) flatResult).listenForUpdates(flatRecorder);
                synchronized (recorders) {
                    recorders.clear();
                    recorders.add(flatRecorder);
                    manage(flatRecorder);
                }
                ((DynamicTable) input).removeUpdateListener(inputRecorder.getValue());
                unmanage(inputRecorder.getValue());
                inputRecorder.setValue(null);
                inputTransformer = null;

                // noinspection unchecked
                resultColumns
                    .forEach((name, scs) -> scs.setNewCurrent(flatResult.getColumnSource(name)));

                index.clear();
                index.insert(flatResult.getIndex());

                final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
                downstream.removed = index.getPrevIndex();
                downstream.added = index.clone();
                downstream.modified = Index.FACTORY.getEmptyIndex();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                downstream.shifted = IndexShiftData.EMPTY;

                conversions.incrementAndGet();

                result.notifyListeners(downstream);
            }

            @Override
            protected boolean canExecute(final long step) {
                synchronized (recorders) {
                    return super.canExecute(step);
                }
            }
        };
        result.addParentReference(mergedListener);

        return result;
    }

}
