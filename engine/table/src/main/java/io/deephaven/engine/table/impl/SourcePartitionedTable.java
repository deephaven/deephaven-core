/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SingleTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;

import java.util.*;

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * {@link PartitionedTable} of single-location {@link SourceTable}s keyed by {@link TableLocationKey}. Refer to
 * {@link TableLocationKey} for an explanation of partitioning.
 */
public class SourcePartitionedTable extends PartitionedTableImpl {

    private static final String KEY_COLUMN_NAME = "TableLocationKey";
    private static final String CONSTITUENT_COLUMN_NAME = "LocationTable";

    /**
     * Construct a {@link SourcePartitionedTable} from the supplied parameters.
     * <p>
     * Note that refreshLocations and refreshSizes are distinct because there are use cases that supply an external
     * RowSet and hence don't require size refreshes. Others might care for size refreshes, but only the
     * initially-available set of locations.
     *
     * @param constituentDefinition The {@link TableDefinition} expected of constituent {@link Table tables}
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows
     * @param tableLocationProvider Source for table locations
     * @param refreshLocations Whether the set of locations should be refreshed
     * @param refreshSizes Whether the locations found should be refreshed
     * @param locationKeyMatcher Function to filter desired location keys
     */
    public SourcePartitionedTable(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean refreshLocations,
            final boolean refreshSizes,
            @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
        super(new UnderlyingTableMaintainer(
                constituentDefinition,
                applyTablePermissions,
                tableLocationProvider,
                refreshLocations,
                refreshSizes,
                locationKeyMatcher).result(),
                Set.of(KEY_COLUMN_NAME),
                true,
                CONSTITUENT_COLUMN_NAME,
                constituentDefinition,
                refreshLocations,
                false);
    }

    private static final class UnderlyingTableMaintainer {

        private final TableDefinition constituentDefinition;
        private final UnaryOperator<Table> applyTablePermissions;
        private final TableLocationProvider tableLocationProvider;
        private final boolean refreshSizes;
        private final Predicate<ImmutableTableLocationKey> locationKeyMatcher;

        private final TrackingWritableRowSet resultRows;
        private final ArrayBackedColumnSource<TableLocationKey> resultTableLocationKeys;
        private final ArrayBackedColumnSource<Table> resultLocationTables;
        private final QueryTable result;

        private final UpdateSourceCombiner refreshCombiner;
        private final TableLocationSubscriptionBuffer subscriptionBuffer;
        private final IntrusiveDoublyLinkedQueue<PendingLocationState> pendingLocationStates;
        private final IntrusiveDoublyLinkedQueue<PendingLocationState> readyLocationStates;
        @SuppressWarnings("FieldCanBeLocal") // We need to hold onto this reference for reachability purposes.
        private final Runnable processNewLocationsUpdateRoot;

        private UnderlyingTableMaintainer(
                @NotNull final TableDefinition constituentDefinition,
                @NotNull final UnaryOperator<Table> applyTablePermissions,
                @NotNull final TableLocationProvider tableLocationProvider,
                final boolean refreshLocations,
                final boolean refreshSizes,
                @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
            this.constituentDefinition = constituentDefinition;
            this.applyTablePermissions = applyTablePermissions;
            this.tableLocationProvider = tableLocationProvider;
            this.refreshSizes = refreshSizes;
            this.locationKeyMatcher = locationKeyMatcher;

            // noinspection resource
            resultRows = RowSetFactory.empty().toTracking();
            resultTableLocationKeys = ArrayBackedColumnSource.getMemoryColumnSource(TableLocationKey.class, null);
            resultLocationTables = ArrayBackedColumnSource.getMemoryColumnSource(Table.class, null);
            final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>(2);
            resultSources.put(KEY_COLUMN_NAME, resultTableLocationKeys);
            resultSources.put(CONSTITUENT_COLUMN_NAME, resultLocationTables);
            result = new QueryTable(resultRows, resultSources);
            result.setFlat();

            final boolean needToRefreshLocations = refreshLocations && tableLocationProvider.supportsSubscriptions();
            if (needToRefreshLocations || refreshSizes) {
                result.setRefreshing(true);
                refreshCombiner = new UpdateSourceCombiner();
                result.addParentReference(refreshCombiner);
            } else {
                refreshCombiner = null;
            }

            if (needToRefreshLocations) {
                subscriptionBuffer = new TableLocationSubscriptionBuffer(tableLocationProvider);
                pendingLocationStates = new IntrusiveDoublyLinkedQueue<>(
                        IntrusiveDoublyLinkedNode.Adapter.<PendingLocationState>getInstance());
                readyLocationStates = new IntrusiveDoublyLinkedQueue<>(
                        IntrusiveDoublyLinkedNode.Adapter.<PendingLocationState>getInstance());
                processNewLocationsUpdateRoot = new InstrumentedUpdateSource(
                        SourcePartitionedTable.class.getSimpleName() + '[' + tableLocationProvider + ']'
                                + "-processPendingLocations") {
                    @Override
                    protected void instrumentedRefresh() {
                        processPendingLocations(true);
                    }
                };
                result.addParentReference(processNewLocationsUpdateRoot);
                refreshCombiner.addSource(processNewLocationsUpdateRoot);
                processPendingLocations(false);
            } else {
                subscriptionBuffer = null;
                pendingLocationStates = null;
                readyLocationStates = null;
                processNewLocationsUpdateRoot = null;
                tableLocationProvider.refresh();
                try (final RowSet added = sortAndAddLocations(tableLocationProvider.getTableLocationKeys().stream()
                        .filter(locationKeyMatcher)
                        .map(tableLocationProvider::getTableLocation))) {
                    resultRows.insert(added);
                }
            }

            if (result.isRefreshing()) {
                // noinspection ConstantConditions
                UpdateGraphProcessor.DEFAULT.addSource(refreshCombiner);
            }
        }

        private QueryTable result() {
            return result;
        }

        private RowSet sortAndAddLocations(@NotNull final Stream<TableLocation> locations) {
            final long initialLastRowKey = resultRows.lastRowKey();
            final MutableLong lastInsertedRowKey = new MutableLong(initialLastRowKey);
            locations.sorted(Comparator.comparing(TableLocation::getKey)).forEach(tl -> {
                final long constituentRowKey = lastInsertedRowKey.incrementAndGet();
                final Table constituentTable = makeConstituentTable(tl);
                resultTableLocationKeys.set(constituentRowKey, tl.getKey());
                resultLocationTables.set(constituentRowKey, constituentTable);
                result.manage(constituentTable);
            });
            return initialLastRowKey == lastInsertedRowKey.longValue()
                    ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(initialLastRowKey, lastInsertedRowKey.longValue());
        }

        private Table makeConstituentTable(@NotNull final TableLocation tableLocation) {
            return applyTablePermissions.apply(new PartitionAwareSourceTable(
                    constituentDefinition,
                    "SingleLocationSourceTable-" + tableLocation,
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    new SingleTableLocationProvider(tableLocation),
                    refreshSizes ? refreshCombiner : null));
        }

        private void processPendingLocations(final boolean notifyListeners) {
            /*
             * This block of code is unfortunate, because it largely duplicates the intent and effort of similar code in
             * RegionedColumnSourceManager. I think that the RegionedColumnSourceManager could be changed to
             * intermediate between TableLocationProvider and SourceTable or SourcePartitionedTable, allowing for much
             * cleaner code in all three. The RCSM could then populate STM nodes or ST regions. We could also add a
             * "RegionManager" to RegionedColumnSources, in order to eliminate the unnecessary post-initialization array
             * population in STM ColumnSources.
             */
            // TODO (https://github.com/deephaven/deephaven-core/issues/867): Refactor around a ticking partition table
            subscriptionBuffer.processPending().stream().filter(locationKeyMatcher)
                    .map(tableLocationProvider::getTableLocation).map(PendingLocationState::new)
                    .forEach(pendingLocationStates::offer);
            for (final Iterator<PendingLocationState> iter = pendingLocationStates.iterator(); iter.hasNext();) {
                final PendingLocationState pendingLocationState = iter.next();
                if (pendingLocationState.exists()) {
                    iter.remove();
                    readyLocationStates.offer(pendingLocationState);
                }
            }
            final RowSet added = sortAndAddLocations(readyLocationStates.stream().map(PendingLocationState::release));
            resultRows.insert(added);
            if (notifyListeners) {
                result.notifyListeners(new TableUpdateImpl(added,
                        RowSetFactory.empty(), RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            } else {
                added.close();
            }
            readyLocationStates.clearFast();
        }
    }

    private static class PendingLocationState extends IntrusiveDoublyLinkedNode.Impl<PendingLocationState> {

        private final TableLocation location;

        private final TableLocationUpdateSubscriptionBuffer subscriptionBuffer;

        private PendingLocationState(@NotNull final TableLocation location) {
            this.location = location;
            subscriptionBuffer = new TableLocationUpdateSubscriptionBuffer(location);
        }

        /**
         * Test if the pending location is ready for inclusion in the result table. This means it must have non-null,
         * non-zero size. We expect that this means the location will be immediately included in the resulting table's
         * {@link ColumnSourceManager}, which is a
         * {@link io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager} in all cases.
         *
         * @return Whether this location exists for purposes of inclusion in the result table
         */
        private boolean exists() {
            subscriptionBuffer.processPending();
            final long localSize = location.getSize();
            // noinspection ConditionCoveredByFurtherCondition
            return localSize != TableLocationState.NULL_SIZE && localSize > 0;
        }

        /**
         * Get rid of the underlying subscription in this state, and return the location.
         *
         * @return The location
         */
        private TableLocation release() {
            subscriptionBuffer.reset();
            return location;
        }
    }
}
