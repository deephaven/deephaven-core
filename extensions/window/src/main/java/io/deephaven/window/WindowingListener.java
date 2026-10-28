package io.deephaven.window;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.time.DateTimeUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.PriorityQueue;
import org.jetbrains.annotations.NotNull;

public class WindowingListener extends InstrumentedTableUpdateListenerAdapter {
    private final QueryTable child;
    private final TrackingWritableRowSet childRowSet;
    private final WritableColumnSource<Instant> childWindowSource;
    private final WritableColumnSource<QueryTable> childTableSource;

    private final Table parent;
    private final ColumnSource<Instant> parentWindowSource;
    private final ColumnSource<QueryTable> parentTableSource;

    private final PriorityQueue<Tracker> tracking;
    private final boolean upperBin;
    private final Duration windowPeriod;
    private final long windowCount;

    protected WindowingListener(
            final Table parent, final QueryTable child,
            final TrackingWritableRowSet childRowSet,
            final ColumnSource<Instant> parentWindowSource,
            final ColumnSource<QueryTable> parentTableSource,
            final WritableColumnSource<Instant> childWindowSource,
            final WritableColumnSource<QueryTable> childTableSource,
            final boolean upperBin, final Duration windowPeriod,
            final long windowCount) {
        super("io-deephaven-window-windowinglistener", parent, false);
        this.parent = parent;
        this.parentWindowSource = parentWindowSource;
        this.parentTableSource = parentTableSource;
        this.child = child;
        this.childRowSet = childRowSet;
        this.childTableSource = childTableSource;
        this.childWindowSource = childWindowSource;
        this.upperBin = upperBin;
        this.windowPeriod = windowPeriod;
        this.windowCount = windowCount;
        this.tracking = new PriorityQueue<>();

        // Add ourselves as a listener of our parent.
        this.parent.addUpdateListener(this);

        // Add ourselves as a parent to our child.
        //
        // Note: Not 100% sure this is necessary as we are installing a constituent
        // dependency one level above but this doesn't seem to hurt anything so
        // leaving it in.
        this.child.addParentReference(this);
    }

    public synchronized void close() {
        this.parent.removeUpdateListener(this);
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        // We explicitly ignore any removes, and we panic on any shifted/modifed
        // data from upstream.
        Assert.eqTrue(upstream.shifted().empty(),
                "blink upstream has shifted data");
        Assert.eqTrue(upstream.modified().isEmpty(),
                "blink upstream has modified data");

        // Grab our current and then earliest possible bucket we care about.
        final long period = this.windowPeriod.toNanos();
        final Instant currentBucket =
                (this.upperBin) ? DateTimeUtils.upperBin(DateTimeUtils.now(), period)
                        : DateTimeUtils.lowerBin(DateTimeUtils.now(), period);
        final Instant earliestBucket =
                (this.windowCount <= 1)
                        ? currentBucket
                        : DateTimeUtils.minus(currentBucket,
                                period * (this.windowCount - 1));

        // Start by cleaning up our local state using a priority queue as a min-heap
        // implementation, allowing for efficient cleanup of existing windows that
        // have expired since the last execution.
        final RowSet removed = this.cleanup(earliestBucket);
        final RowSet added = this.insert(upstream.added(), earliestBucket);

        // Update our current rowset with adds/removes, and then notify our child
        // tables listeners.
        this.childRowSet.update(added, removed);
        this.child.notifyListeners(added, removed, RowSetFactory.empty());
    }

    private RowSet insert(@NotNull final RowSet added,
            @NotNull final Instant earliestBucket) {
        // Create a new rowset builder so we can inform our downstream of anything
        // that eventually gets inserted.
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        // This is unfortunately a O(n) operation, and it sort of has to be as we
        // need to check each and every one of the new window rows to see if we care
        // about them. At any rate it should be that `n` is sufficiently small that
        // this is not an issue; One would have to want to window at microsecond
        // intervals or smaller for this to really be an issue.
        added.forAllRowKeys((key) -> {
            // For each key start by grabbing the window we are working with and then
            // verify we care about it.
            final Instant window = this.parentWindowSource.get(key);
            if (DateTimeUtils.isBefore(window, earliestBucket)) {
                return;
            }

            // Since we care about this table, grab it and place it in _our_ output
            // sources, this will allow us to manage it properly. Also we need to make
            // sure we add back the Blink table attribute that we had to remove to
            // support the partitioned_agg_by call.
            final QueryTable table =
                    (QueryTable) this.parentTableSource.get(key).withAttributes(
                            Map.of(Table.BLINK_TABLE_ATTRIBUTE, Boolean.TRUE));

            // Quickly ensure that we have enough capacity for this new table to
            // track.
            this.childTableSource.ensureCapacity(key + 1);
            this.childWindowSource.ensureCapacity(key + 1);

            // Finally inserting the values we need in the various column sources plus
            // our priority queue for pending cleanup.
            this.childWindowSource.set(key, window);
            this.childTableSource.set(key, table);
            this.tracking.add(new Tracker(key, window));

            // Lastly ensure we notify our downstreams that there is a new key to work
            // with.
            builder.addKey(key);
        });

        return builder.build();
    }

    private RowSet cleanup(@NotNull final Instant earliestBucket) {
        // Create a new rowset builder so we can inform our downstream of anything
        // that eventually gets removed.
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();

        // Check our current list of tables and nuke anything that should be
        // removed.
        while (!this.tracking.isEmpty()) {
            // Start by taking a peak at the oldest tracker we have.
            if (DateTimeUtils.isAfterOrEqual(this.tracking.peek().window,
                    earliestBucket)) {
                // If we get here nothing else could possibly need pruning thanks to the
                // priority queue, just break and be done with it.
                break;
            }

            // Actually pop off the oldest and null out any data at the corresponding
            // key, while making sure we notify our downstreams that the remove
            // happened.
            final Tracker oldest = this.tracking.poll();
            this.childWindowSource.setNull(oldest.key);
            this.childTableSource.setNull(oldest.key);
            builder.addKey(oldest.key);
        }

        return builder.build();
    }

    protected class Tracker implements Comparable<Tracker> {
        public long key;
        public Instant window;

        protected Tracker(long key, Instant window) {
            this.key = key;
            this.window = window;
        }

        @Override
        public int compareTo(Tracker other) {
            // Compare based on the priority field
            return Long.compare(DateTimeUtils.epochNanos(this.window),
                    DateTimeUtils.epochNanos(other.window));
        }
    }
}
