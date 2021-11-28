package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SingleTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;

import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * {@link LocalTableMap} of single-location {@link SourceTable}s keyed by {@link TableLocationKey}. Refer to
 * {@link TableLocationKey} for an explanation of partitioning.
 */
public class SourceTableMap extends LocalTableMap {

    private final UnaryOperator<Table> applyTablePermissions;
    private final TableLocationProvider tableLocationProvider;
    private final boolean refreshSizes;
    private final Predicate<ImmutableTableLocationKey> locationKeyMatcher;

    private final UpdateSourceCombiner refreshCombiner;
    private final TableLocationSubscriptionBuffer subscriptionBuffer;
    private final IntrusiveDoublyLinkedQueue<PendingLocationState> pendingLocationStates;
    private final IntrusiveDoublyLinkedQueue<PendingLocationState> readyLocationStates;
    @SuppressWarnings("FieldCanBeLocal") // We need to hold onto this reference for reachability purposes.
    private final Runnable processNewLocationsUpdateRoot;

    /**
     * <p>
     * Construct a {@link SourceTableMap} from the supplied parameters.
     *
     * <p>
     * Note that refreshLocations and refreshSizes are distinct because there are use cases that supply an external
     * RowSet and hence don't require size refreshes. Others might care for size refreshes, but only the
     * initially-available set of locations.
     *
     * @param tableDefinition The table definition
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows
     * @param tableLocationProvider Source for table locations
     * @param refreshLocations Whether the set of locations should be refreshed
     * @param refreshSizes Whether the locations found should be refreshed
     * @param locationKeyMatcher Function to filter desired location keys
     */
    public SourceTableMap(@NotNull final TableDefinition tableDefinition,
            @NotNull final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean refreshLocations,
            final boolean refreshSizes,
            @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
        super(null, Objects.requireNonNull(tableDefinition));
        this.applyTablePermissions = applyTablePermissions;
        this.tableLocationProvider = tableLocationProvider;
        this.refreshSizes = refreshSizes;
        this.locationKeyMatcher = locationKeyMatcher;

        final boolean needToRefreshLocations = refreshLocations && tableLocationProvider.supportsSubscriptions();

        if (needToRefreshLocations || refreshSizes) {
            setRefreshing(true);
            refreshCombiner = new UpdateSourceCombiner();
            manage(refreshCombiner);
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
                    SourceTableMap.class.getSimpleName() + '[' + tableLocationProvider + ']'
                            + "-processPendingLocations") {
                @Override
                protected void instrumentedRefresh() {
                    processPendingLocations();
                }
            };
            refreshCombiner.addSource(processNewLocationsUpdateRoot);
            processPendingLocations();
        } else {
            subscriptionBuffer = null;
            pendingLocationStates = null;
            readyLocationStates = null;
            processNewLocationsUpdateRoot = null;
            tableLocationProvider.refresh();
            sortAndAddLocations(tableLocationProvider.getTableLocationKeys().stream().filter(locationKeyMatcher)
                    .map(tableLocationProvider::getTableLocation));
        }

        if (isRefreshing()) {
            // noinspection ConstantConditions
            UpdateGraphProcessor.DEFAULT.addSource(refreshCombiner);
        }
    }

    private void sortAndAddLocations(@NotNull final Stream<TableLocation> locations) {
        // final value we can use to detect not-created tables
        final MutableBoolean observeCreation = new MutableBoolean(false);
        locations.sorted(Comparator.comparing(TableLocation::getKey)).forEach(tl -> {
            observeCreation.setValue(false);
            final Table previousTable = computeIfAbsent(tl.getKey(), o -> {
                observeCreation.setValue(true);
                return makeTable(tl);
            });

            if (!observeCreation.getValue()) {
                // we have a duplicate location - not allowed
                final TableLocation previousLocation =
                        ((PartitionAwareSourceTable) previousTable).locationProvider.getTableLocation(tl.getKey());
                throw new TableDataException(
                        "Data Routing Configuration error: TableDataService elements overlap at location " +
                                tl.toGenericString() +
                                ". Duplicate locations are " + previousLocation.toStringDetailed() + " and "
                                + tl.toStringDetailed());
            }
        });
    }

    private Table makeTable(@NotNull final TableLocation tableLocation) {
        return applyTablePermissions.apply(new PartitionAwareSourceTable(
                getConstituentDefinition().orElseThrow(IllegalStateException::new),
                "SingleLocationSourceTable-" + tableLocation,
                RegionedTableComponentFactoryImpl.INSTANCE,
                new SingleTableLocationProvider(tableLocation),
                refreshSizes ? refreshCombiner : null));
    }

    private void processPendingLocations() {
        // This block of code is unfortunate, because it largely duplicates the intent and effort of similar code in
        // RegionedColumnSourceManager. I think that the RegionedColumnSourceManager could be changed to intermediate
        // between TableLocationProvider and SourceTable or SourceTableMap, allowing for much cleaner code in all three.
        // The RCSM could then populate STM nodes or ST regions. We could also add a "RegionManager" to
        // RegionedColumnSources, in order to eliminate the unnecessary post-initialization array population in STM
        // ColumnSources.
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
        sortAndAddLocations(readyLocationStates.stream().map(PendingLocationState::release));
        readyLocationStates.clearFast();
    }

    private static class PendingLocationState extends IntrusiveDoublyLinkedNode.Impl<PendingLocationState> {

        private final TableLocation location;

        private final TableLocationUpdateSubscriptionBuffer subscriptionBuffer;

        private PendingLocationState(@NotNull final TableLocation location) {
            this.location = location;
            subscriptionBuffer = new TableLocationUpdateSubscriptionBuffer(location);
        }

        /**
         * Test if the pending location is ready for inclusion in the table map. This means it must have non-null,
         * non-zero size. We expect that this means the location will be immediately included in the resulting table's
         * {@link ColumnSourceManager}, which is a
         * {@link io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager} in all cases.
         *
         * @return Whether this location exists for purposes of inclusion in the table map
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
