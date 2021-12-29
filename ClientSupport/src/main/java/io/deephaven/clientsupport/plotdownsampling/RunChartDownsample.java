package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.time.DateTime;
import io.deephaven.hash.KeyedLongObjectHash;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.function.LongNumericPrimitives;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Downsamples a table assuming its contents will be rendered in a run chart, with the each subsequent row holding a
 * later X value (i.e. is sorted on that column). Multiple Y columns can be specified, as can a range of values for the
 * X value (providing a "zoom" feature, with high resolution data in that range, and low resolution data outside of it).
 */
/*
 * TODO Remaining work to make this class more efficient. This work can be done incrementally as we find we need
 * specific cases to be faster, but at the time of writing, this is "fast enough" for updating, appending tables with
 * 10m+ rows in them to look good in the web UI: o switching downsample<->passthrough is very untested, likely buggy
 * (PRESENTLY DISABLED) o support automatic re-ranging, due to too many items being added/removed o read MCS on updates
 * to decide whether or not to even check for changes o handle non-QueryTable instances o make shifting more efficient o
 * make nulls result in fewer items in the result table
 */
public class RunChartDownsample implements Function<Table, Table> {
    private static final Logger log = LoggerFactory.getLogger(RunChartDownsample.class);

    public static final int CHUNK_SIZE = Configuration.getInstance().getIntegerWithDefault("chunkSize", 1 << 14);

    /** Enable this to add additional checks at runtime. */
    private static final boolean VALIDATE =
            Configuration.getInstance().getBooleanForClassWithDefault(RunChartDownsample.class, "validate", false);
    private static final String BUCKET_SIZES_KEY = RunChartDownsample.class.getSimpleName() + ".bucketsizes";
    /**
     * Specifies the bucket sizes to round up to when a client specifies some number of pixels. If empty, each user will
     * get exactly the size output table that they asked for, but this likely will not be memoized
     */
    private static final int[] BUCKET_SIZES = Configuration.getInstance().hasProperty(BUCKET_SIZES_KEY)
            ? Configuration.getInstance().getIntegerArray(BUCKET_SIZES_KEY)
            : new int[] {500, 1000, 2000, 4000};

    private final int pxCount;
    private final long[] zoomRange;
    private final String xColumnName;
    private final String[] yColumnNames;

    public RunChartDownsample(final int pxCount, @Nullable final long[] zoomRange, final String xColumnName,
            final String[] yColumnNames) {
        Assert.gt(pxCount, "pxCount", 0);
        Assert.neqNull(xColumnName, "xColumnName");
        Assert.neqNull(yColumnNames, "yColumnNames");
        Assert.gt(yColumnNames.length, "yColumnNames.length", 0);
        if (zoomRange != null) {
            Assert.eq(zoomRange.length, "zoomRange.length", 2);
            Assert.lt(zoomRange[0], "zoomRange[0]", zoomRange[1], "zoomRange[1]");
        }
        this.pxCount = pxCount;
        this.zoomRange = zoomRange;
        this.xColumnName = xColumnName;
        this.yColumnNames = yColumnNames;
    }

    @Override
    public Table apply(final Table wholeTable) {
        final int minBins = findMatchingBinSize(pxCount);

        if (wholeTable instanceof QueryTable) {
            final QueryTable wholeQueryTable = (QueryTable) wholeTable;

            return QueryPerformanceRecorder.withNugget(
                    "downsample(" + minBins + ", " + xColumnName + " {" + Arrays.toString(yColumnNames) + "})",
                    wholeQueryTable.sizeForInstrumentation(), () -> {
                        final DownsampleKey memoKey = new DownsampleKey(minBins, xColumnName, yColumnNames, zoomRange);
                        return wholeQueryTable.memoizeResult(memoKey,
                                () -> makeDownsampledQueryTable(wholeQueryTable, memoKey));
                    });
        }

        // TODO restore this to support non-QueryTable types
        // if (wholeTable instanceof BaseTable) {
        // BaseTable baseTable = (BaseTable) wholeTable;
        // final SwapListener swapListener =
        // baseTable.createSwapListenerIfRefreshing(SwapListener::new);
        //
        // final Mutable<QueryTable> result = new MutableObject<>();
        //
        // baseTable.initializeWithSnapshot("downsample", swapListener, (prevRequested, beforeClock) -> {
        // final boolean usePrev = prevRequested && baseTable.isRefreshing();
        // final WritableRowSet rowSetToUse = usePrev ? baseTable.build().copyPrev() : baseTable.build();
        //
        // // process existing rows
        // handleAdded(rowSetToUse, columnSourceToBin, getNanosPerPx(minBins, usePrev, rowSetToUse, columnSourceToBin),
        // valueColumnSource, states, usePrev);
        //
        // // construct the initial row set and table
        // //TODO copy def, columns that we actually need here
        // QueryTable resultTable = new QueryTable(buildIndexFromGroups(states), baseTable.getColumnSourceMap());
        // result.setValue(resultTable);
        //
        // return true;
        // });
        // return result.getValue();
        // }

        throw new IllegalArgumentException("Can't downsample table of type " + wholeTable.getClass());
    }

    private Table makeDownsampledQueryTable(final QueryTable wholeQueryTable, final DownsampleKey memoKey) {
        final SwapListener swapListener =
                wholeQueryTable.createSwapListenerIfRefreshing(SwapListener::new);

        final Mutable<Table> result = new MutableObject<>();

        wholeQueryTable.initializeWithSnapshot("downsample", swapListener, (prevRequested, beforeClock) -> {
            final boolean usePrev = prevRequested && wholeQueryTable.isRefreshing();

            final DownsamplerListener downsampleListener = DownsamplerListener.of(wholeQueryTable, memoKey);
            downsampleListener.init(usePrev);
            result.setValue(downsampleListener.resultTable);

            if (swapListener != null) {
                swapListener.setListenerAndResult(downsampleListener, downsampleListener.resultTable);
                downsampleListener.resultTable.addParentReference(swapListener);
                downsampleListener.resultTable.addParentReference(downsampleListener);
            }

            return true;
        });

        return result.getValue();
    }

    private final static class DownsampleKey extends MemoizedOperationKey {
        private final int bins;
        private final String xColumnName;
        private final String[] yColumnNames;
        private final long[] zoomRange;

        private DownsampleKey(final int bins, final String xColumnName, final String[] yColumnNames,
                final long[] zoomRange) {
            this.bins = bins;
            this.xColumnName = xColumnName;
            this.yColumnNames = yColumnNames;
            this.zoomRange = zoomRange;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof DownsampleKey))
                return false;

            final DownsampleKey that = (DownsampleKey) o;

            if (bins != that.bins)
                return false;
            if (!xColumnName.equals(that.xColumnName))
                return false;
            if (!Arrays.equals(yColumnNames, that.yColumnNames))
                return false;
            return zoomRange != null ? Arrays.equals(zoomRange, that.zoomRange) : that.zoomRange == null;
        }

        @Override
        public int hashCode() {
            int result = bins;
            result = 31 * result + xColumnName.hashCode();
            result = 31 * result + Arrays.hashCode(yColumnNames);
            result = 31 * result + (zoomRange != null ? Arrays.hashCode(zoomRange) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "DownsampleKey{" +
                    "bins=" + bins +
                    ", xColumnName='" + xColumnName + '\'' +
                    ", yColumnNames=" + Arrays.toString(yColumnNames) +
                    ", zoomRange=" + Arrays.toString(zoomRange) +
                    '}';
        }
    }

    private static int findMatchingBinSize(final int minBins) {
        return IntStream.of(BUCKET_SIZES)
                .filter(bin -> bin >= minBins)
                .findFirst()
                .orElse(minBins);
    }

    private static class DownsamplerListener extends BaseTable.ListenerImpl {

        private enum IndexMode {
            PASSTHROUGH, DOWNSAMPLE
        }
        private enum RangeMode {
            ZOOM, AUTO
        }

        public static DownsamplerListener of(final QueryTable sourceTable, final DownsampleKey key) {
            final TrackingRowSet rowSet = RowSetFactory.empty().toTracking();
            final QueryTable resultTable = sourceTable.getSubTable(rowSet);
            return new DownsamplerListener(sourceTable, resultTable, key);
        }

        private final QueryTable sourceTable;
        private final TrackingWritableRowSet rowSet;
        private final QueryTable resultTable;
        private final DownsampleKey key;
        private IndexMode indexMode = IndexMode.DOWNSAMPLE;
        private final RangeMode rangeMode;

        private long nanosPerPx;

        private BucketState head;
        private BucketState tail;

        private final List<ColumnSource<?>> valueColumnSources;
        private final ColumnSource<Long> xColumnSource;

        private final ValueTracker[] values;
        private final WritableRowSet availableSlots = RowSetFactory.builderSequential().build();
        private int nextSlot;

        private final int[] allYColumnIndexes;

        private final KeyedLongObjectHashMap<BucketState> states =
                new KeyedLongObjectHashMap<>(new KeyedLongObjectKey.BasicLax<BucketState>() {
                    @Override
                    public long getLongKey(final BucketState bucketState) {
                        return bucketState.getKey();
                    }
                });

        private final KeyedLongObjectHash.ValueFactory<BucketState> bucketStateFactory =
                new KeyedLongObjectHash.ValueFactory<BucketState>() {
                    @Override
                    public BucketState newValue(final long key) {
                        return new BucketState(key, nextPosition(), values, true);
                    }

                    @Override
                    public BucketState newValue(final Long key) {
                        return newValue((long) key);
                    }
                };

        private DownsamplerListener(final QueryTable sourceTable, final QueryTable resultTable,
                final DownsampleKey key) {
            super("downsample listener", sourceTable, resultTable);
            this.sourceTable = sourceTable;
            this.rowSet = resultTable.getRowSet().writableCast();
            this.resultTable = resultTable;
            this.key = key;

            final ColumnSource xSource = sourceTable.getColumnSource(key.xColumnName);
            if (xSource.getType() == DateTime.class) {
                this.xColumnSource = ReinterpretUtils.dateTimeToLongSource(xSource);
            } else if (xSource.allowsReinterpret(long.class)) {
                // noinspection unchecked
                this.xColumnSource = xSource.reinterpret(long.class);
            } else {
                throw new IllegalArgumentException(
                        "Cannot use non-DateTime, non-long x column " + key.xColumnName + " in downsample");
            }

            this.valueColumnSources = Arrays.stream(this.key.yColumnNames)
                    .map(colName -> (ColumnSource<?>) this.sourceTable.getColumnSource(colName))
                    .collect(Collectors.toList());

            // pre-size the array sources, indicate that these indexes are available as bucketstates are created
            // always leave 0, 1 for head/tail, we start counting at 2
            nextSlot = key.bins + 2;
            this.values = ValueTracker.of(valueColumnSources, nextSlot);
            availableSlots.insertRange(2, nextSlot - 1);

            if (key.zoomRange != null) {
                rangeMode = RangeMode.ZOOM;

                head = new BucketState(-1, 0, values, false);
                tail = new BucketState(-1, 1, values, false);
            } else {
                rangeMode = RangeMode.AUTO;
            }
            allYColumnIndexes = IntStream.range(0, key.yColumnNames.length).toArray();
        }

        @Override
        protected void destroy() {
            super.destroy();
            states.values().forEach(BucketState::close);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            try (final DownsampleChunkContext context =
                    new DownsampleChunkContext(xColumnSource, valueColumnSources, CHUNK_SIZE)) {
                handleRemoved(context, upstream.removed());

                handleShifts(upstream.shifted());

                handleAdded(context, upstream.added());

                handleModified(context, upstream.modified(), upstream.modifiedColumnSet());

                performRescans(context);

                notifyResultTable(upstream);

                // TODO Complete this so we can switch modes. In the meantime, this is wrapped in if(false)
                // so that if other changes happen in the APIs this uses, local code won't break. When
                // implemented, remove the operations above, and inline the method.
                // maybeSwitchModes(upstream, context);

                if (VALIDATE) {
                    if (rangeMode == RangeMode.ZOOM) {
                        head.validate(false, context, allYColumnIndexes);
                        tail.validate(false, context, allYColumnIndexes);
                    }
                    states.values().forEach(state -> state.validate(false, context, allYColumnIndexes));
                    if (!rowSet.subsetOf(sourceTable.getRowSet())) {
                        throw new IllegalStateException("rowSet.subsetOf(sourceTable.build()) is false, extra items= "
                                + rowSet.minus(sourceTable.getRowSet()));
                    }
                }

            }

        }

        @SuppressWarnings("unused")
        protected void maybeSwitchModes(TableUpdate upstream, DownsampleChunkContext context) {
            // Consider switching modes - this is deliberately hard to swap back and forth between
            if (indexMode == IndexMode.PASSTHROUGH
                    && sourceTable.size() > key.bins * 2 * (2 + key.yColumnNames.length)) {
                log.info().append("Switching from PASSTHROUGH to DOWNSAMPLE ").append(sourceTable.size())
                        .append(key.toString()).endl();
                // If there are more than 4x items in the source table as there are PX to draw, convert to downsampled
                indexMode = IndexMode.DOWNSAMPLE;

                // act as if all items were just added fresh
                rerange();
                handleAdded(context, sourceTable.getRowSet());

                Assert.assertion(this.rowSet.isEmpty(), "this.rowSet.empty()");

                // notify downstream tables that the row set was swapped
                notifyResultTable(upstream, sourceTable.getRowSet());
            } else if (indexMode == IndexMode.DOWNSAMPLE && sourceTable.size() < key.bins) {
                log.info().append("Switching from DOWNSAMPLE to PASSTHROUGH ").append(sourceTable.size())
                        .append(key.toString()).endl();
                // if the table has shrunk until there are less items in the table than there are Px to draw, just show
                // the items
                indexMode = IndexMode.PASSTHROUGH;

                states.clear();

                final RowSet addToResultTable = sourceTable.getRowSet().minus(rowSet);
                final RowSet removed = rowSet.union(upstream.removed());
                final RowSet modified = rowSet.union(upstream.modified());
                rowSet.clear();

                availableSlots.clear();// TODO optionally, clear out value trackers?
                nextSlot = 1;

                // notify downstream tables that the row set changed, add all missing rows from the source table, since
                // we're un-downsampling
                // TODO
                final TableUpdate switchToPassThrough =
                        new TableUpdateImpl(addToResultTable, removed, modified, upstream.shifted(),
                                upstream.modifiedColumnSet());
                resultTable.notifyListeners(switchToPassThrough);
            } else if (indexMode == IndexMode.PASSTHROUGH) {
                log.info().append("PASSTHROUGH update ").append(upstream).endl();
                // send the event along directly
                Assert.assertion(this.rowSet.isEmpty(), "this.rowSet.empty()");
                resultTable.notifyListeners(upstream);
            } else {
                log.info().append("DOWNSAMPLE update ").append(upstream).endl();
                // indexMode is DOWNSAMPLE

                if (rangeMode == RangeMode.ZOOM) {
                    // no need to rebucket the focused portion.
                    // won't rebucket head or tail, for now we just have one bucket at all times
                    // TODO reconsider this. also, implement this.

                    handleRemoved(context, upstream.removed());

                    handleShifts(upstream.shifted());

                    handleAdded(context, upstream.added());

                    handleModified(context, upstream.modified(), upstream.modifiedColumnSet());

                    performRescans(context);

                    notifyResultTable(upstream);
                } else {
                    // // Decide if it is time to re-bucket. As above, we want this to infrequently done, but we also
                    // don't
                    // // want to bump into the next bucket size up or down. Right now, the bucket count should start at
                    // // 110% of the requested PX size, and can shrink to 100%, or grow to up to 150%.
                    // // We rebucket _before_ getting any work done, since otherwise we would bucket twice in this one
                    // pass.
                    // // This does seem a bit silly, but we're assuming that if the last change pushed us over the edge
                    // so
                    // // that we need to rebucket, it still makes sense to do so. With that said, we don't use the last
                    // // update's ranges when rebucketing, we'll start fresh.
                    // if (we needed to rebucket last time && still need to rebucket) {
                    // rerange();
                    // states.clear();
                    // rowSet.clear();
                    // availableSlots.clear();//TODO tell the value trackers to nuke content?
                    // nextSlot = 1;
                    // handleAdded(sourceTable.build());
                    //
                    // notifyResultTable(upstream);
                    // } else {
                    handleRemoved(context, upstream.removed());

                    handleShifts(upstream.shifted());

                    handleAdded(context, upstream.added());

                    handleModified(context, upstream.modified(), upstream.modifiedColumnSet());

                    performRescans(context);

                    notifyResultTable(upstream);
                    // }

                }
            }
        }

        public void init(final boolean usePrev) {
            rerange(usePrev);
            try (final DownsampleChunkContext context =
                    new DownsampleChunkContext(xColumnSource, valueColumnSources, CHUNK_SIZE)) {
                handleAdded(context, usePrev, sourceTable.getRowSet());
                if (VALIDATE) {
                    Consumer<BucketState> validate = state -> {
                        // prebuild the RowSet so we can log details
                        state.makeRowSet();

                        // check that the state is sane
                        state.validate(usePrev, context, allYColumnIndexes);
                    };
                    states.values().forEach(validate);
                    if (rangeMode == RangeMode.ZOOM) {
                        validate.accept(head);
                        validate.accept(tail);
                    }
                }
            }

            Assert.assertion(rowSet.isEmpty(), "this.rowSet.empty()");
            final RowSet initialRowSet = indexFromStates();
            // log.info().append("initial downsample rowSet.size()=").append(initialRowSet.size()).append(",
            // rowSet=").append(initialRowSet).endl();

            rowSet.insert(initialRowSet);
        }

        private int nextPosition() {
            if (availableSlots.isEmpty()) {
                int nextPosition = nextSlot++;
                for (ValueTracker tracker : values) {
                    tracker.ensureCapacity(nextSlot);
                }
                return nextPosition;
            }
            final int reused = (int) availableSlots.firstRowKey();
            availableSlots.remove(reused);
            return reused;
        }

        private void releasePosition(final long position) {
            availableSlots.insert(position);
        }

        private void rerange() {
            rerange(false);
        }

        private void rerange(final boolean usePrev) {
            // read the first and last value in the source table, and work out our new nanosPerPx value
            final long first;
            final long last;
            if (rangeMode == RangeMode.ZOOM) {
                first = key.zoomRange[0];
                last = key.zoomRange[1];
            } else {
                final RowSet rowSet = usePrev ? sourceTable.getRowSet().copyPrev() : sourceTable.getRowSet();
                first = xColumnSource.getLong(rowSet.firstRowKey());
                last = xColumnSource.getLong(rowSet.lastRowKey());
            }

            // take the difference between first and last, divide by the requested number of bins,
            // and add 10% so we can lose some without reranging right away
            nanosPerPx = (long) (1.1 * (last - first) / key.bins);
        }

        private void handleAdded(final DownsampleChunkContext context, final boolean usePrev,
                final RowSet addedRowSet) {
            final RowSet rowSet = usePrev ? addedRowSet.trackingCast().copyPrev() : addedRowSet;
            if (rowSet.isEmpty()) {
                return;
            }

            // since we're adding values, we need all contexts
            final int[] all = this.allYColumnIndexes;
            context.addYColumnsOfInterest(all);

            final RowSequence.Iterator it = rowSet.getRowSequenceIterator();
            while (it.hasMore()) {
                final RowSequence next = it.getNextRowSequenceWithLength(CHUNK_SIZE);
                final LongChunk<Values> xValueChunk = context.getXValues(next, usePrev);
                final LongChunk<OrderedRowKeys> keyChunk = next.asRowKeyChunk();
                final Chunk<? extends Values>[] valueChunks = context.getYValues(all, next, usePrev);

                long lastBin = 0;
                BucketState bucket = null;
                for (int indexInChunk = 0; indexInChunk < xValueChunk.size(); indexInChunk++) {
                    final long xValue = xValueChunk.get(indexInChunk);
                    final long bin = LongNumericPrimitives.lowerBin(xValue, nanosPerPx);

                    if (lastBin != bin || bucket == null) {
                        bucket = getOrCreateBucket(bin);
                        lastBin = bin;
                    }
                    bucket.append(keyChunk.get(indexInChunk), valueChunks, indexInChunk);
                }
            }
        }

        private void handleAdded(final DownsampleChunkContext context, final RowSet addedRowSet) {
            handleAdded(context, false, addedRowSet);
        }

        private void handleRemoved(final DownsampleChunkContext context, final RowSet removed) {
            if (removed.isEmpty()) {
                return;
            }

            final RowSequence.Iterator it = removed.getRowSequenceIterator();

            while (it.hasMore()) {
                final RowSequence next = it.getNextRowSequenceWithLength(CHUNK_SIZE);
                final LongChunk<Values> dateChunk = context.getXValues(next, true);
                final LongChunk<OrderedRowKeys> keyChunk = next.asRowKeyChunk();

                final long lastBin = 0;
                BucketState bucket = null;
                for (int i = 0; i < dateChunk.size(); i++) {
                    final long bin = LongNumericPrimitives.lowerBin(dateChunk.get(i), nanosPerPx);

                    if (lastBin != bin || bucket == null) {
                        bucket = getBucket(bin);
                    }
                    bucket.remove(keyChunk.get(i));
                }
            }

        }

        private void handleModified(final DownsampleChunkContext context, final RowSet modified,
                final ModifiedColumnSet modifiedColumnSet) {
            // TODO use MCS here
            if (modified.isEmpty()/* || !modifiedColumnSet.containsAny(interestedColumns) */) {
                return;
            }

            // figure out which columns we're interested in that were changed
            // TODO wire this into the context, and the current MCS
            final int[] yColIndexes = allYColumnIndexes;

            // build the chunk GetContexts, if needed
            context.addYColumnsOfInterest(yColIndexes);

            final RowSequence.Iterator it = modified.getRowSequenceIterator();

            while (it.hasMore()) {
                final RowSequence next = it.getNextRowSequenceWithLength(CHUNK_SIZE);
                final LongChunk<Values> oldDateChunk = context.getXValues(next, true);
                final LongChunk<Values> newDateChunk = context.getXValues(next, false);
                final LongChunk<OrderedRowKeys> keyChunk = next.asRowKeyChunk();
                final Chunk<? extends Values>[] valueChunks = context.getYValues(yColIndexes, next, false);

                final long lastBin = 0;
                BucketState bucket = null;
                for (int indexInChunk = 0; indexInChunk < oldDateChunk.size(); indexInChunk++) {
                    final long bin = LongNumericPrimitives.lowerBin(oldDateChunk.get(indexInChunk), nanosPerPx);
                    final long newBin = LongNumericPrimitives.lowerBin(newDateChunk.get(indexInChunk), nanosPerPx);

                    if (lastBin != bin || bucket == null) {
                        bucket = getBucket(bin);
                    }
                    final long rowIndex = keyChunk.get(indexInChunk);
                    if (bin != newBin) {
                        // item moved between buckets
                        bucket.remove(rowIndex);
                        getOrCreateBucket(newBin).append(rowIndex, valueChunks, indexInChunk);
                        return;
                    } else {
                        bucket.update(rowIndex, valueChunks, indexInChunk);
                    }
                }
            }
        }

        private BucketState getOrCreateBucket(final long bin) {
            if (rangeMode == RangeMode.ZOOM) {
                if (bin + nanosPerPx < key.zoomRange[0]) {
                    return head;
                }
                if (bin > key.zoomRange[1]) {
                    return tail;
                }
            }
            return states.putIfAbsent(bin, bucketStateFactory);
        }

        private BucketState getBucket(final long bin) {
            // long bin = LongNumericPrimitives.lowerBin(xValue, nanosPerPx);
            if (rangeMode == RangeMode.ZOOM) {
                if (bin + nanosPerPx < key.zoomRange[0]) {
                    return head;
                }
                if (bin > key.zoomRange[1]) {
                    return tail;
                }
            }
            BucketState bucket = states.get(bin);
            if (bucket == null) {
                throw new IllegalStateException("Failed to find state for bucket " + bin);
            }
            return bucket;
        }

        private void handleShifts(final RowSetShiftData shiftData) {
            for (final BucketState bucketState : states.values()) {
                bucketState.shift(shiftData);
            }
        }

        private void performRescans(final DownsampleChunkContext context) {
            // check each group to see if any needs a rescan
            for (final Iterator<BucketState> iterator = states.values().iterator(); iterator.hasNext();) {
                final BucketState bucket = iterator.next();
                if (bucket.getRowSet().isEmpty()) {
                    // if it has no keys at all, remove it so we quit checking it
                    iterator.remove();
                    releasePosition(bucket.getOffset());
                    bucket.close();
                } else {
                    bucket.rescanIfNeeded(context);
                }
            }
        }

        /**
         * Indicates that a change has probably happened and we should notify the result table. The contents of the
         * change will be our state map (i.e. there is
         * 
         * @param upstream the change that happened upstream
         * @param lastRowSet the base rowSet to use when considering what items to tell the result table changed. if
         *        this.rowSet, then update it normally, otherwise this.rowSet must be empty and this.rowSet should be
         *        populated.
         */
        private void notifyResultTable(final TableUpdate upstream, final RowSet lastRowSet) {
            final RowSet resultRowSet = indexFromStates();

            final RowSet removed = lastRowSet.minus(resultRowSet);
            final RowSet added = resultRowSet.minus(lastRowSet);
            final RowSet modified = resultRowSet.intersect(lastRowSet).intersect(upstream.modified());

            if (lastRowSet == this.rowSet) {
                this.rowSet.update(added, removed);
            } else {
                // switching modes, need to populate the rowSet
                Assert.assertion(this.rowSet.isEmpty(), "this.rowSet.empty()");
                this.rowSet.insert(resultRowSet);
            }
            // log.info().append("After downsample update, rowSet.size=").append(rowSet.size()).append(",
            // rowSet=").endl();//.append(rowSet).endl();

            final TableUpdate update =
                    new TableUpdateImpl(added, removed, modified, upstream.shifted(), upstream.modifiedColumnSet());
            // log.info().append("resultTable.notifyListeners").append(update).endl();
            resultTable.notifyListeners(update);
        }

        private RowSet indexFromStates() {
            // TODO this couldnt be uglier if i tried
            if (rangeMode == RangeMode.ZOOM) {
                return Stream.concat(
                        Stream.of(head, tail).filter(s -> !s.getRowSet().isEmpty()), // note: we only filter these two,
                                                                                     // since states shouldn't contain
                                                                                     // empty indexes anyway
                        states.values().stream())
                        .reduce(RowSetFactory.builderRandom(), (builder, state) -> {
                            builder.addRowSet(state.makeRowSet());
                            return builder;
                        }, (b1, b2) -> {
                            b1.addRowSet(b2.build());
                            return b1;
                        }).build();
            }
            return states.values().stream().reduce(RowSetFactory.builderRandom(), (builder, state) -> {
                builder.addRowSet(state.makeRowSet());
                return builder;
            }, (b1, b2) -> {
                b1.addRowSet(b2.build());
                return b1;
            }).build();
        }

        private void notifyResultTable(final TableUpdate upstream) {
            notifyResultTable(upstream, rowSet);
        }
    }

}
