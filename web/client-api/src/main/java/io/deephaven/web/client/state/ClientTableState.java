//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.state;

import elemental2.core.JsMap;
import elemental2.core.JsObject;
import elemental2.core.JsSet;
import elemental2.core.Uint8Array;
import elemental2.promise.Promise;
import io.deephaven.chunk.ChunkType;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.batch.TableConfig;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.state.HasTableState;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.fu.*;
import jsinterop.base.Js;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.web.client.fu.JsItr.iterate;

/**
 * Container for state information pertaining to a given {@link TableTicket}.
 *
 * Where JsTable is a mutable object which can point to any given ClientTableState, each ClientTableState represents an
 * immutable table configuration / handle which can have zero or more JsTable objects bound to it.
 *
 * This type is used to replace Table#StackEntry, and works with TableList to form an arbitrary "chain of table
 * mutations leading to table state at handle N".
 *
 * Each JsTable maintains their own TableList / linking structure of ClientTableState instances, and each CTS holds maps
 * of "the state each bound JsTable holds -for this particular handle-".
 *
 * Being mutable, a JsTable can change its binding to any given CTS instance, and will need to temporarily abandon it's
 * current state when transitioning to a new one. We need to be able to reinstate the last good active state whenever a
 * request fails, otherwise we will releaseTable from the given state.
 *
 * Once no JsTable is making any use of any ClientTableState, that state should be released on the server and discarded
 * by the client (an interim state which an active state is based on must always be retained).
 *
 * By making the JsTable read it's state from a ClientTableState, switching to another state should be relatively
 * seamless; JsTable is mutable and as stateless as possible, with as much state as possible shoved into CTS (so it can
 * go away and be restored sanely).
 *
 * Consider making this a js type with restricted, read-only property access.
 */
public final class ClientTableState extends TableConfig {

    public enum ResolutionState {
        /**
         * Table has been created on the client, but client does not yet have a handle ID referring to the table on the
         * server
         */
        UNRESOLVED,
        /**
         * Table exists on both client and server, but isn't yet ready to be used - no definition has been received.
         */
        RESOLVED,
        /**
         * Table exists on both the client and the server, and is able to be used. This is sort-of-terminal, as it may
         * change during a reconnect.
         */
        RUNNING,
        /**
         * The table no longer exists on the server, and will not be recreated. This is a terminal state.
         */
        RELEASED,
        /**
         * The table failed to be created on the server and so cannot be used. This is sort-of-terminal, as it may
         * change during a reconnect.
         */
        FAILED
    }

    public static final long SIZE_UNINITIALIZED = -1;

    // ===================================================================================
    // The fields which describe the identity semantics of a ClientTableState object
    // are all in the super type TableConfig, except for the table handle.
    // These will participate in hashCode and equals; all properties
    // which can result in a change of handle should be included in TableConfig.
    // ===================================================================================
    private final TableTicket handle;
    private final WorkerConnection connection;
    private final JsTableFetch fetch;
    private final String fetchSummary;

    // ===================================================================================
    // These fields describe mutable value state of a ClientTableState
    // (these will not participate in hashCode and equals)
    // ===================================================================================

    private volatile ResolutionState resolution;

    // All bindings for this state.
    private final JsMap<JsTable, ActiveTableBinding> active = new JsMap<>();
    private final JsMap<JsTable, PausedTableBinding> paused = new JsMap<>();
    // Objects of any type that want to cause a CTS to be retained.
    // Once all active, paused and retainer referrers are cleared,
    // The state will be released and deleted.
    private final JsSet<Object> retainers = new JsSet<>();

    // Lazily compute fast lookup maps on demand
    private JsLazy<Map<String, Column>> columnLookup;

    // Some callbacks for when this state resolves
    private List<JsConsumer<ClientTableState>> onRunning;
    private List<JsConsumer<String>> onFailed;
    private List<JsRunnable> onReleased;

    // A bit of state management
    private String failMsg;
    private Double queuedSize;

    // Leftovers from Table.StackEntry
    // non-final fields, but can only be set once (consider moving these into a bean of their own)
    private Column[] columns;
    private Column[] allColumns; // includes invisible columns
    private JsLayoutHints layoutHints;
    private JsTotalsTableConfig totalsTableConfig;
    private long size;
    private InitialTableDefinition tableDef;
    private Column rowFormatColumn;
    private boolean isStatic;

    /**
     * We maintain back-links to our source state.
     *
     * Any state can have N other states after it, but only one state before it.
     *
     * We maintain this linked list so we can reconstitute a valid TableList from any given table state (TableList
     * maintains it's own linking structure on top of states, as each Table will use a private TableList to track it's
     * own state stack).
     *
     */
    private final ClientTableState source;

    private double touch;
    private boolean stayDead;

    public ClientTableState(
            WorkerConnection connection,
            TableTicket handle,
            JsTableFetch fetcher,
            String fetchSummary) {
        this(connection, handle, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, fetcher, fetchSummary);
    }

    private ClientTableState(
            WorkerConnection connection,
            TableTicket handle,
            List<Sort> sort,
            List<String> conditions,
            List<FilterCondition> filter,
            List<CustomColumnDescriptor> customColumns,
            List<String> dropColumns,
            List<String> viewColumns,
            ClientTableState source,
            JsTableFetch fetch,
            String fetchSummary) {
        super(sort, conditions, filter, customColumns, dropColumns, viewColumns);
        this.touch = System.currentTimeMillis();
        this.fetch = fetch;
        this.connection = connection;
        this.handle = handle;
        this.size = SIZE_UNINITIALIZED; // make sure a "change" to 0 fires an event.
        columnLookup = resetLookup();
        this.resolution = handle.isResolved() ? ResolutionState.RESOLVED : ResolutionState.UNRESOLVED;
        this.source = source;

        this.fetchSummary = fetchSummary;
    }

    public Promise<ClientTableState> maybeRevive(BrowserHeaders metadata) {
        connection.scheduleCheck(this);
        if (isEmpty()) {
            JsLog.debug("Skipping revive as state is empty");
            return Promise.resolve(this);
        }
        if (isRunning() && !isDisconnected()) {
            JsLog.debug("Skipping revive as state is running");
            return Promise.resolve(this);
        }

        // revive!
        return refetch(null, metadata);
    }

    private JsLazy<Map<String, Column>> resetLookup() {
        return new JsLazy<>(() -> {
            Map<String, Column> lookup = new LinkedHashMap<>();
            if (columns != null) {
                for (Column column : columns) {
                    lookup.put(column.getName(), column);
                }
            }
            return lookup;
        });
    }

    public boolean hasDefinition() {
        return tableDef != null;
    }

    public boolean isRunning() {
        if (resolution == ResolutionState.RUNNING) {
            assert handle.isResolved() || isDisconnected() : "Handle not resolved when running";
            assert columns != null : "Null columns resolved when running";
            assert tableDef != null : "Null tableDef resolved when running";
            return true;
        }
        return false;
    }

    public boolean isResolved() {
        return getHandle().isResolved();
    }

    public TableTicket getHandle() {
        return handle;
    }

    /**
     * Returns the ChunkType to use for each column in the table. This is roughly
     * {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToWritablePrimitiveChunkType(Class)}
     * but without the trip through Class. Note also that effectively all types are stored as Objects except non-long
     * primitives, so that they can be appropriately wrapped before storing (though the storage process will handle DH
     * nulls).
     */
    public ChunkType[] chunkTypes() {
        return Arrays.stream(columnTypes()).map(dataType -> {
            if (dataType == Boolean.class || dataType == boolean.class) {
                return ChunkType.Object;
            }
            if (dataType == Long.class || dataType == long.class) {
                // JS client holds longs as LongWrappers
                return ChunkType.Object;
            }
            return ChunkType.fromElementType(dataType);
        }).toArray(ChunkType[]::new);
    }

    /**
     * Returns the Java Class to represent each column in the table. This lets the client replace certain JVM-only
     * classes with alternative implementations, but still use the simple
     * {@link io.deephaven.extensions.barrage.chunk.ChunkReader.TypeInfo} wrapper.
     */
    public Class<?>[] columnTypes() {
        return Arrays.stream(tableDef.getColumns())
                .map(ColumnDefinition::getType)
                .map(t -> {
                    switch (t) {
                        case "boolean":
                        case "java.lang.Boolean":
                            return boolean.class;
                        case "char":
                        case "java.lang.Character":
                            return char.class;
                        case "byte":
                        case "java.lang.Byte":
                            return byte.class;
                        case "int":
                        case "java.lang.Integer":
                            return int.class;
                        case "short":
                        case "java.lang.Short":
                            return short.class;
                        case "long":
                        case "java.lang.Long":
                            return long.class;
                        case "java.lang.Float":
                        case "float":
                            return float.class;
                        case "java.lang.Double":
                        case "double":
                            return double.class;
                        case "java.time.Instant":
                            return DateWrapper.class;
                        case "java.math.BigInteger":
                            return BigIntegerWrapper.class;
                        case "java.math.BigDecimal":
                            return BigDecimalWrapper.class;
                        default:
                            return Object.class;
                    }
                })
                .toArray(Class<?>[]::new);
    }

    /**
     * Returns the Java Class to represent the component type in any list/array type. At this time, this value is not
     * used by the chunk reading implementation.
     */
    public Class<?>[] componentTypes() {
        return Arrays.stream(tableDef.getColumns()).map(ColumnDefinition::getType).map(t -> {
            // All arrays and vectors will be handled as objects for now.
            // TODO (deephaven-core#2102) clarify if we need to handle these cases at all
            if (t.endsWith("[]") || t.endsWith("Vector")) {
                return Object.class;
            }
            // Non-arrays or vectors should return null
            return null;
        }).toArray(Class[]::new);
    }

    public ClientTableState newState(TableTicket newHandle, TableConfig config) {
        if (config == null) {
            config = this;
        }
        final List<Sort> sorts = config.getSorts();
        final List<String> conditions = config.getConditions();
        final List<FilterCondition> filters = config.getFilters();
        final List<CustomColumnDescriptor> customColumns = config.getCustomColumns();
        final List<String> dropColumns = config.getDropColumns();
        final List<String> viewColumns = config.getViewColumns();

        final ClientTableState newState = new ClientTableState(
                connection, newHandle, sorts, conditions, filters, customColumns, dropColumns, viewColumns, this,
                null, config.toSummaryString());
        newState.setFlat(config.isFlat());
        if (!isRunning()) {
            onFailed(reason -> newState.setResolution(ResolutionState.FAILED, reason), JsRunnable.doNothing());
        }
        touch();
        return newState;
    }

    private void touch() {
        touch = System.currentTimeMillis();
    }

    public ResolutionState getResolution() {
        return resolution;
    }

    public void setResolution(ResolutionState resolution) {
        assert resolution != ResolutionState.FAILED : "Failures must provide a reason!";
        setResolution(resolution, null);
    }

    public void setResolution(ResolutionState resolution, String failMsg) {
        if (this.resolution == ResolutionState.RELEASED) {
            assert resolution == ResolutionState.RELEASED : "Trying to unrelease CTS " + this + " to " + resolution;
            return;
        }
        if (this.resolution == resolution) {
            return;
        }
        this.resolution = resolution;
        if (resolution == ResolutionState.RUNNING) {
            if (onRunning != null) {
                onRunning.forEach(c -> c.apply(this));
                onRunning = null;
            }
            onFailed = null;
            onReleased = null;
        } else if (resolution == ResolutionState.FAILED) {
            assert Js.isTruthy(failMsg) : "Failures must provide a reason!";
            this.failMsg = failMsg;
            if (onFailed != null) {
                onFailed.forEach(c -> c.apply(failMsg));
                onFailed = null;
            }
            // after a failure, we should discard onRunning (and this state entirely)
            onRunning = null;
            onReleased = null;

            forActiveTables(t -> t.failureHandled(failMsg));
        } else if (resolution == ResolutionState.RELEASED) {
            if (onReleased != null) {
                onReleased.forEach(JsRunnable::run);
                onReleased = null;
            }
            onRunning = null;
            onFailed = null;
        }
    }

    public List<String> getCustomColumnsString() {
        return getCustomColumns().stream().map(CustomColumnDescriptor::getExpression)
                .collect(Collectors.toList());
    }

    public List<CustomColumn> getCustomColumnsObject() {
        return getCustomColumns().stream().map(CustomColumn::new)
                .collect(Collectors.toList());
    }

    public Column[] getColumns() {
        return columns;
    }

    public Column[] getAllColumns() {
        return allColumns;
    }

    public JsLayoutHints getLayoutHints() {
        if (layoutHints == null) {
            createLayoutHints();
        }
        return layoutHints;
    }

    public JsTotalsTableConfig getTotalsTableConfig() {
        if (totalsTableConfig == null) {
            createTotalsTableConfig();
        }
        return totalsTableConfig;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        if (this.size == Long.MIN_VALUE) {
            // Table is uncoalesced, ignore size change
            return;
        }
        boolean log = this.size != size;
        if (log) {
            JsLog.debug("CTS", this, " set size; was ", this.size, " is now ", size);
        }
        this.size = size;

        if (isRunning()) {
            if (log) {
                JsLog.debug("CTS immediate size update ", this.size, " actives: ", active);
            }
            forActiveTables(t -> t.setSize(size));
        } else {
            if (queuedSize == null) {
                JsLog.debug("Queuing size changed until RUNNING STATE; ", size);
                onRunning(self -> {
                    JsLog.debug("Firing queued size change event (", queuedSize, ")");
                    forActiveTables(table -> {
                        table.setSize(size);
                        queuedSize = null;
                    });
                }, JsRunnable.doNothing());
            }
            queuedSize = (double) size;
        }
    }

    public InitialTableDefinition getTableDef() {
        return tableDef;
    }

    private void setTableDef(InitialTableDefinition tableDef) {
        boolean create = this.tableDef != tableDef;
        this.tableDef = tableDef;
        if (create) {
            ColumnDefinition[] columnDefinitions = tableDef.getColumns();

            // iterate through the columns, combine format columns into the normal model
            Map<Boolean, Map<String, ColumnDefinition>> byNameMap = tableDef.getColumnsByName();
            Column[] columns = new Column[0];
            allColumns = new Column[0];
            for (ColumnDefinition definition : columnDefinitions) {
                Column column = definition.makeJsColumn(columns.length, byNameMap);
                if (definition.isForRow()) {
                    // special case for the row format column
                    setRowFormatColumn(column);
                    continue;
                }

                // note the use of columns.length as jsIndex is accurate for visible columns
                allColumns[allColumns.length] = column;

                if (definition.isVisible()) {
                    columns[columns.length] = allColumns[allColumns.length - 1];
                }
            }

            this.columns = JsObject.freeze(columns);
            this.columnLookup = resetLookup();
        }
    }

    private void createLayoutHints() {
        String hintsString = getTableDef().getAttributes().getLayoutHints();
        JsLayoutHints jsHints = new JsLayoutHints();
        if (hintsString == null) {
            layoutHints = null;
        } else {
            layoutHints = jsHints.parse(hintsString);
        }
    }

    private void createTotalsTableConfig() {
        String configString = getTableDef().getAttributes().getTotalsTableConfig();

        if (configString == null) {
            totalsTableConfig = null;
        } else {
            totalsTableConfig = JsTotalsTableConfig.parse(configString);
        }
    }

    public Column getRowFormatColumn() {
        return rowFormatColumn;
    }

    public void setRowFormatColumn(Column rowFormatColumn) {
        this.rowFormatColumn = rowFormatColumn;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && handle.equals(((ClientTableState) o).handle);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + handle.hashCode();
        return result;
    }

    public void onRunning(JsConsumer<ClientTableState> callback, JsRunnable other) {
        onRunning(callback, other.asConsumer(), other);
    }

    public void onRunning(JsConsumer<ClientTableState> callback, JsConsumer<String> failed, JsRunnable release) {
        if (resolution == ResolutionState.RELEASED) {
            release.run();
            return;
        } else if (resolution == ResolutionState.FAILED) {
            failed.apply(failMsg);
            return;
        }
        if (resolution == ResolutionState.RUNNING) {
            // wait a moment and execute the callback, after verifying that we remained in that state
            LazyPromise.runLater(() -> {
                if (resolution == ResolutionState.RUNNING) {
                    callback.apply(this);
                } else {
                    // Resubmit the callback? Probably not, this means we momentarily were in the correct state, but
                    // if we return to it, things could get weird
                    // onRunning(callback);
                }
            });
            return;
        }
        if (onRunning == null) {
            onRunning = new ArrayList<>();
        }
        assert !onRunning.contains(callback) : "Double-added callback";
        onRunning.add(callback);

        if (onFailed == null) {
            onFailed = new ArrayList<>();
        }
        onFailed.add(failed);
        if (onReleased == null) {
            onReleased = new ArrayList<>();
        }
        onReleased.add(release);
    }

    public void onFailed(JsConsumer<String> callback, JsRunnable other) {
        if (resolution == ResolutionState.RELEASED || resolution == ResolutionState.RUNNING) {
            other.run();
            return;
        }
        if (resolution == ResolutionState.FAILED) {
            LazyPromise.runLater(() -> {
                if (resolution == ResolutionState.FAILED) {
                    callback.apply(failMsg);
                } else {
                    // Resubmit the callback? Probably not, this means we momentarily were in the correct state, but
                    // if we return to it, things could get weird
                    // onFailed(callback);
                }
            });
            return;
        }
        if (onFailed == null) {
            onFailed = new ArrayList<>();
        }
        assert !onFailed.contains(callback) : "Double-added callback";
        onFailed.add(callback);

        if (onRunning == null) {
            onRunning = new ArrayList<>();
        }
        onRunning.add(other.asConsumer());
        if (onReleased == null) {
            onReleased = new ArrayList<>();
        }
        onReleased.add(other);
    }

    /**
     * Checks if the current state can be used as a root to turn into the state described by the supplied arguments.
     *
     * While it is technically valid to interleave filters and sorts, for maximal performance, we want to ensure filters
     * are always applied before sorts; so, if you have added a filter, but the current state has any sorts at all, we
     * will return false, even though this state would be correct when used, it won't be maximally efficient.
     *
     * Additionally, custom columns might be used in filters and sorts, but a filter or sort on a custom column cannot
     * be created until that custom column has been exported and provides a table definition so that we can supply
     * Column instances to operate upon.
     *
     * @param sorts The full set of sorts we want in our resulting table state
     * @param filters The full set of filters we want in our resulting table state
     * @param customColumns The full set of custom columns we want in our resulting table state
     * @param flat True if the resulting table should have a flatten index, false otherwise
     * @return true if this state can be used to add
     */
    public boolean isCompatible(List<Sort> sorts, List<FilterCondition> filters,
            List<CustomColumnDescriptor> customColumns, boolean flat) {
        // start with the easy wins...
        if (isEmptyConfig()) {
            // an empty root state is compatible to everyone.
            return true;
        }

        // we can't be compatible if we have more sorts, filters or custom columns than requested.
        if (getSorts().size() > sorts.size()) {
            return false;
        }
        if (getFilters().size() > filters.size()) {
            return false;
        }
        if (getCustomColumns().size() > customColumns.size()) {
            return false;
        }

        // sorts cannot be appended to in any way; we can only use different sorts when
        // the current state has at least a
        boolean sortsInequal = !getSorts().equals(sorts);
        // requested sorts start with our sorts
        boolean maybeSortOnly = false;
        if (sortsInequal) {
            maybeSortOnly = startsWith(sorts, getSorts());
            if (maybeSortOnly) {
                if (!getSorts().isEmpty()) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // filters do not need to be in the same order
        if (!filters.containsAll(getFilters())) {
            return false;
        }
        // custom columns must be an exact prefix of the requested columns (we already checked size above)
        if (!startsWith(customColumns, getCustomColumns())) {
            return false;
        }

        // when a sort is already applied, unless filters are exactly equal,
        // we are going to consider ourselves incompatible.
        // this is a performance optimization; we are better off to throw away intermediate
        // tables than to perform filters after any sorts.
        if (getSorts().size() > 0 && getFilters().size() != filters.size()) {
            // note, if we have sorts applied and filter lengths are the same, but with different contents,
            // we will have already failed the isSuperSet check above
            return false;
        }

        if (maybeSortOnly) {
            // we already tested the reverse above.
            if (!getFilters().containsAll(filters)) {
                return false;
            }
            if (!customColumns.equals(getCustomColumns())) {
                return false;
            }
        }

        if (!flat && isFlat()) {
            return false;
        }

        return true;
    }

    private <T> boolean startsWith(List<T> candidate, List<T> prefix) {
        return prefix.equals(candidate.subList(0, prefix.size()));
    }

    public Column findColumn(String key) {
        Column c = columnLookup.get().get(key);
        if (c == null) {
            // we could consider making this forgiving, but it's better if clients don't send us garbage requests.
            throw new NoSuchElementException(key);
        }
        return c;
    }

    /**
     * @return true if there are no tables bound to this state. If a table that had a subscription for this state was
     *         orphaned by a pending request, we want to clear the subscription immediately so it becomes inert
     *         (immediately remove the subscription), but we may need to rollback the request, and we don't want to
     *         release the handle until the pending request is finished (whereupon we will remove the binding).
     */
    public boolean isEmpty() {
        return active.size == 0 && paused.size == 0 && retainers.size == 0;
    }

    /**
     * Call this with a marker object that you want to use to cause the state to be retained. You either need to hold
     * onto the returned function to clear your binding, or call {@link #unretain(Object)} when finished to avoid
     * keeping this handle alive beyond its usefulness.
     */
    public JsRunnable retain(Object retainer) {
        retainers.add(retainer);
        connection.scheduleCheck(this);
        return () -> unretain(retainer);
    }

    public void unretain(Object retainer) {
        JsLog.debug("Unretainment", retainer, " releasing ", LazyString.of(this::toStringMinimal));
        retainers.delete(retainer);
        connection.scheduleCheck(this);
    }

    public boolean hasSort(Sort candidate) {
        return getSorts().contains(candidate);
    }

    public boolean hasCustomColumn(CustomColumnDescriptor candidate) {
        return getCustomColumns().contains(candidate);
    }

    public boolean hasFilter(FilterCondition candidate) {
        return getFilters().contains(candidate);
    }

    public boolean isFinished() {
        return resolution == ResolutionState.RUNNING || resolution == ResolutionState.FAILED
                || resolution == ResolutionState.RELEASED;
    }

    public boolean isDisconnected() {
        return !getHandle().isConnected();
    }

    public void addPaused(JsTable table, PausedTableBinding paused) {
        assert resolution != ResolutionState.RELEASED;
        assert !active.has(table);
        assert !this.paused.has(table) || this.paused.get(table) == paused;
        this.paused.set(table, paused);
    }

    public ActiveTableBinding createBinding(JsTable table) {
        assert resolution != ResolutionState.RELEASED;
        ActiveTableBinding sub = ActiveTableBinding.create(table, this);
        active.set(table, sub);
        touch();
        return sub;
    }

    public boolean releaseTable(JsTable table) {
        assert !active.has(table) : "Cannot releaseTable from active table!";
        boolean had = paused.has(table);
        if (had) {
            paused.delete(table);
            connection.scheduleCheck(this);
            if (!isEmpty()) {
                touch();
            }
        }
        return had;
    }

    public BitSet makeBitset(Column[] columns) {
        BitSet bitSet = new BitSet(getTableDef().getColumns().length);
        Arrays.stream(columns).flatMapToInt(Column::getRequiredColumns).forEach(bitSet::set);
        if (getRowFormatColumn() != null) {
            bitSet.set(getRowFormatColumn().getIndex());
        }
        return bitSet;
    }


    public MappedIterable<JsTable> getBoundTables() {
        assert iterate(active.keys()).noneMatch(paused::has) : "State cannot be active and paused at the same time; "
                + "active: " + active + " paused: " + paused;
        return iterate(active.keys()).plus(iterate(paused.keys()));
    }

    public void forActiveTables(JsConsumer<JsTable> callback) {
        JsItr.forEach(active, (table, sub) -> {
            callback.apply(table);
        });
    }

    @SuppressWarnings("unchecked")
    public void forActiveLifecycles(JsConsumer<HasLifecycle> callback) {
        JsSet<HasLifecycle> items = new JsSet<>();
        for (JsTable o : JsItr.iterate(active.keys())) {
            items.add(o);
        }
        for (Object o : JsItr.iterate(retainers.values())) {
            if (o instanceof HasLifecycle) {
                items.add((HasLifecycle) o);
            } else {
                JsLog.debug("Ignoring non-lifecycle retainer", o);
            }
        }
        for (HasLifecycle item : JsItr.iterate(items.values())) {
            callback.apply(item);
        }
    }

    @Override
    public String toString() {
        return "ClientTableState{" +
                "handle=" + handle +
                ", resolution=" + resolution +
                ", active=" + active +
                ", paused=" + paused +
                ", size=" + size +
                ", tableDef=" + tableDef +
                ", rowFormatColumn=" + rowFormatColumn +
                ", failMsg='" + failMsg + '\'' +
                ", sorts=" + getSorts() +
                ", filters=" + getFilters() +
                ", customColumns=" + getCustomColumns() +
                ", selectDistinct=" + getSelectDistinct() +
                "} ";
    }

    public String toStringMinimal() {
        return "ClientTableState{" +
                "handle=" + handle +
                ", resolution=" + resolution +
                ", active=" + active.size +
                ", paused=" + paused.size +
                ", retainers=" + retainers.size +
                ", size=" + size +
                ", tableDef=" + tableDef +
                ", rowFormatColumn=" + rowFormatColumn +
                "} ";
    }

    public ClientTableState getPrevious() {
        return source;
    }

    public HasTableState<ClientTableState> getBinding(JsTable table) {
        HasTableState<ClientTableState> sub = active.get(table);
        if (sub == null) {
            sub = paused.get(table);
        }
        return sub;
    }

    public ActiveTableBinding getActiveBinding(JsTable table) {
        final HasTableState<ClientTableState> existing = getBinding(table);
        return existing == null ? null
                : existing.isActive() ? (ActiveTableBinding) existing
                        : ((PausedTableBinding) existing).getActiveBinding();
    }

    public Iterable<ActiveTableBinding> getActiveBindings() {
        return JsItr.iterate(active.values());
    }

    public void pause(JsTable table) {
        final ActiveTableBinding was = active.get(table);
        if (was != null) {
            // we were active, and now we're paused.
            active.delete(table);
            paused.set(table, was.getPaused());
            connection.scheduleCheck(this);
        }
    }

    public void unpause(JsTable table) {
        final PausedTableBinding was = paused.get(table);
        assert was != null : "Cannot unpause a table that is not paused " + this;
        paused.delete(table);
        active.set(table, was.getActiveBinding());
        // Now, we want to put back the viewport, if any, since those may be still used by the original table
        ActiveTableBinding sub = was.getActiveBinding();
        table.maybeReviveSubscription();
    }

    public MappedIterable<ClientTableState> ancestors() {
        return new LinkedIterable<>(this, ClientTableState::getPrevious, true);
    }

    public MappedIterable<ClientTableState> reversed() {
        return new LinkedIterable<>(this, ClientTableState::getPrevious, false);
    }

    /**
     * Look through paused tables to see if any of them have been closed.
     */
    public void cleanup() {
        assert JsItr.iterate(active.keys()).allMatch(t -> !t.isAlive() || t.state() == this)
                : "active map not up-to-date with tables";
        if (getResolution() == ResolutionState.RELEASED) {
            for (PausedTableBinding sub : JsItr.iterate(paused.values())) {
                releaseTable(sub.getActiveBinding().getTable());
            }
            active.clear();
            // notify any retainers who have events that we've been released.
            for (Object retainer : JsItr.iterate(retainers.values())) {
                if (retainer instanceof HasEventHandling) {
                    ((HasEventHandling) retainer).fireEvent(HasEventHandling.INTERNAL_EVENT_RELEASED, this);
                }
            }

            retainers.clear();
            return;
        }
        for (PausedTableBinding sub : JsItr.iterate(paused.values())) {
            assert sub.getActiveBinding().getState() == this;
            final JsTable table = sub.getActiveBinding().getTable();
            if (table.isClosed()) {
                releaseTable(table);
            } else if (!table.hasHandle(getHandle()) && !table.hasRollbackHandle(getHandle())) {
                // release the table if it no longer has a reference to the handle with any of it's bindings, including
                // paused bindings
                // don't releaseTable for tables that are in flux
                if (table.state().isRunning()) {
                    releaseTable(table);
                } else {
                    // wait until the table's current state is complete to try cleaning up again.
                    table.state().onRunning(i -> cleanup(), this::cleanup);
                }
            }
        }
        if (isEmpty()) {
            connection.scheduleCheck(this);
        }
    }

    public double getLastTouched() {
        return touch;
    }

    public static Comparator<? super ClientTableState> newestFirst() {
        return (a, b) -> (int) Math.signum(b.getLastTouched() - a.getLastTouched());
    }

    public Promise<JsTable> fetchTable(HasEventHandling failHandler, BrowserHeaders metadata) {
        return refetch(failHandler, metadata).then(cts -> Promise.resolve(new JsTable(connection, cts)));
    }

    public Promise<ClientTableState> refetch(HasEventHandling failHandler, BrowserHeaders metadata) {
        if (fetch == null) {
            if (failMsg != null) {
                return Promise.reject(failMsg);
            }
            return Promise.resolve(this);
        }
        final Promise<ExportedTableCreationResponse> promise =
                Callbacks.grpcUnaryPromise(c -> fetch.fetch(c, this, metadata));
        // noinspection unchecked
        return promise.then(def -> {
            if (resolution == ResolutionState.RELEASED) {
                // was released before we managed to finish the fetch, ignore
                // noinspection rawtypes,unchecked
                return (Promise) Promise.reject(
                        "Table already released, cannot process incoming table definition, this can be safely ignored.");
            }
            applyTableCreationResponse(def);
            return Promise.resolve(this);
        });
    }

    public void applyTableCreationResponse(ExportedTableCreationResponse def) {
        assert def.getResultId().getTicket().getTicket_asB64().equals(getHandle().makeTicket().getTicket_asB64())
                : "Ticket is incompatible with the table details";
        // by definition, the ticket is now exported and connected
        handle.setState(TableTicket.State.EXPORTED);
        handle.setConnected(true);

        Uint8Array flightSchemaMessage = def.getSchemaHeader_asU8();
        isStatic = def.getIsStatic();

        setTableDef(WebBarrageUtils.readTableDefinition(WebBarrageUtils.readSchemaMessage(flightSchemaMessage)));

        setResolution(ResolutionState.RUNNING);
        setSize(Long.parseLong(def.getSize()));
    }

    public boolean isAncestor(ClientTableState was) {
        for (ClientTableState state : new LinkedIterable<>(this, ClientTableState::getPrevious)) {
            if (state == was) {
                return true;
            }
        }

        return false;
    }

    public void doNotResuscitate() {
        stayDead = true;
    }

    public boolean shouldResuscitate() {
        return resolution != ResolutionState.RELEASED && !stayDead;
    }

    public boolean hasRetainer(Object test) {
        return retainers.has(test);
    }

    public WorkerConnection getConnection() {
        return connection;
    }

    public String getFetchSummary() {
        return fetchSummary;
    }

    public boolean isStatic() {
        return isStatic;
    }
}
