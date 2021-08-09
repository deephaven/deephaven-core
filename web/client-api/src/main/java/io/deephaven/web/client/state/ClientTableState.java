package io.deephaven.web.client.state;

import elemental2.core.JsMap;
import elemental2.core.JsObject;
import elemental2.core.JsSet;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.Message;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.MessageHeader;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Field;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.KeyValue;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Schema;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.batch.TableConfig;
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
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.deephaven.web.client.fu.JsItr.iterate;

/**
 * Container for state information pertaining to a given {@link TableHandle}.
 *
 * Where JsTable is a mutable object which can point to any given ClientTableState,
 * each ClientTableState represents an immutable table configuration / handle
 * which can have zero or more JsTable objects bound to it.
 *
 * This type is used to replace Table#StackEntry,
 * and works with TableList to form an arbitrary
 * "chain of table mutations leading to table state at handle N".
 *
 * Each JsTable maintains their own TableList / linking structure of ClientTableState instances,
 * and each CTS holds maps of "the state each bound JsTable holds -for this particular handle-".
 *
 * Being mutable, a JsTable can change its binding to any given CTS instance,
 * and will need to temporarily abandon it's current state when transitioning to a new one.
 * We need to be able to reinstate the last good active state whenever a request fails,
 * otherwise we will releaseTable from the given state.
 *
 * Once no JsTable is making any use of any ClientTableState, that state should
 * be released on the server and discarded by the client (an interim state which
 * an active state is based on must always be retained).
 *
 * By making the JsTable read it's state from a ClientTableState, switching to another
 * state should be relatively seamless; JsTable is mutable and as stateless as possible,
 * with as much state as possible shoved into CTS (so it can go away and be restored sanely).
 *
 * Consider making this a js type with restricted, read-only property access.
 */
public final class ClientTableState extends TableConfig {
    public enum ResolutionState {
        /**
         * Table has been created on the client, but client does not yet have a handle ID referring to the table on the server
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
    private boolean subscribed;
    private Double queuedSize;

    // Leftovers from Table.StackEntry
    //non-final fields, but can only be set once (consider moving these into a bean of their own)
    private Column[] columns;
    private Column[] allColumns; // includes invisible columns
    private long size;
    private InitialTableDefinition tableDef;
    private Column rowFormatColumn;

    /**
     * We maintain back-links to our source state.
     *
     * Any state can have N other states after it, but only one state before it.
     *
     * We maintain this linked list so we can reconstitute a valid TableList from
     * any given table state (TableList maintains it's own linking structure on top
     * of states, as each Table will use a private TableList to track it's own state stack).
     *
     */
    private final ClientTableState source;

    private double touch;
    private boolean stayDead;

    public ClientTableState(
        WorkerConnection connection,
        TableTicket handle,
        JsTableFetch fetcher,
        String fetchSummary
    ) {
        this(connection, handle, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, fetcher, fetchSummary);
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
        String fetchSummary
    ) {
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
        return new JsLazy<>(()->{
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
            (c, s, metadata)->{
                // This fetcher will not be used for the initial fetch, only for refetches.
                // Importantly, any CTS with a source (what we are creating here; source=this, above)
                // is revived, it does not use the refetcher; we directly rebuild batch operations instead.
                // It may make sense to actually have batches route through reviver instead.
                connection.getReviver().revive(metadata, s);
            }, config.toSummaryString()
        );
        newState.setFlat(config.isFlat());
        if (!isRunning()) {
            onFailed(reason->newState.setResolution(ResolutionState.FAILED, reason), JsRunnable.doNothing());
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

    public Column[] getColumns() {
        return columns;
    }

    public Column[] getAllColumns() {
        return allColumns;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        boolean log = this.size != size;
        if (log) {
            JsLog.debug("CTS", this, " set size; was ", this.size, " is now ", size);
        }
        this.size = size;

        if (isRunning()) {
            if (log) {
                JsLog.debug("CTS immediate size update ", this.size, " actives: ", active);
            }
            forActiveTables(table -> table.setSize(size));
        } else {
            if (queuedSize == null) {
                JsLog.debug("Queuing size changed until RUNNING STATE; ", size);
                onRunning(self -> {
                    JsLog.debug("Firing queued size change event (", queuedSize, ")");
                    forActiveTables(table -> {
                        table.setSize(queuedSize);
                    });
                    queuedSize = null;
                }, JsRunnable.doNothing());
            }
            queuedSize = (double)size;
        }
    }

    public InitialTableDefinition getTableDef() {
        return tableDef;
    }

    private void setTableDef(InitialTableDefinition tableDef) {
        boolean create = this.tableDef != tableDef;
        this.tableDef = tableDef;
        if (create) {
            createColumns();
        }
    }

    private void createColumns() {
        ColumnDefinition[] columnDefinitions = tableDef.getColumns();

        //iterate through the columns, combine format columns into the normal model
        Map<String, ColumnDefinition> byNameMap = Arrays.stream(columnDefinitions)
            .collect(columnCollector(false));
        Column[] columns = new Column[0];
        allColumns = new Column[0];
        Map<String, String> columnDescriptions = new HashMap<>();
        String[][] descriptionsArrays = tableDef.getAttributes().getColumnDescriptions();
        if (descriptionsArrays != null) {
            for (int i = 0; i < descriptionsArrays.length; i++) {
                String[] pair = descriptionsArrays[i];
                columnDescriptions.put(pair[0], pair[1]);
            }
        }
        for (ColumnDefinition definition : columnDefinitions) {
            if (definition.isForRow()) {
                //special case for the row format column
                setRowFormatColumn(makeColumn(-1, definition, null, null, false, null, null));
                continue;
            }
            String name = definition.getName();

            ColumnDefinition format = byNameMap.get(definition.getFormatColumnName());
            ColumnDefinition style = byNameMap.get(definition.getStyleColumnName());

            boolean isPartitionColumn = definition.isPartitionColumn();

            // note the use of columns.length as jsIndex is accurate for visible columns
            allColumns[allColumns.length] = makeColumn(columns.length,
                definition,
                format == null || !format.isNumberFormatColumn() ? null : format.getColumnIndex(),
                style == null ? null : style.getColumnIndex(),
                isPartitionColumn,
                format == null || format.isNumberFormatColumn() ? null : format.getColumnIndex(),
                columnDescriptions.get(name)
            );

            if (definition.isVisible()) {
                columns[columns.length] = allColumns[allColumns.length - 1];
            }
        }

        this.columns = JsObject.freeze(columns);
        this.columnLookup = resetLookup();
    }

    private static Column makeColumn(int jsIndex, ColumnDefinition definition, Integer numberFormatIndex, Integer styleIndex, boolean isPartitionColumn, Integer formatStringIndex, String description) {
        return new Column(jsIndex, definition.getColumnIndex(), numberFormatIndex, styleIndex, definition.getType(), definition.getName(), isPartitionColumn, formatStringIndex, description);
    }

    private static Collector<? super ColumnDefinition, ?, Map<String, ColumnDefinition>> columnCollector(boolean ordered) {
        return Collectors.toMap(ColumnDefinition::getName, Function.identity(), assertNoDupes(), ordered ? LinkedHashMap::new : HashMap::new);
    }

    private static <T> BinaryOperator<T> assertNoDupes() {
        return (u, v) -> {
            assert u == v : "Duplicates found for " + u + " and " + v;
            return u;
        };
    }

    public Column getRowFormatColumn() {
        return rowFormatColumn;
    }

    public void setRowFormatColumn(Column rowFormatColumn) {
        this.rowFormatColumn = rowFormatColumn;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && handle.equals(((ClientTableState)o).handle);
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
            LazyPromise.runLater(()-> {
                if (resolution == ResolutionState.RUNNING) {
                    callback.apply(this);
                } else {
                    // Resubmit the callback? Probably not, this means we momentarily were in the correct state, but
                    // if we return to it, things could get weird
//                    onRunning(callback);
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
            LazyPromise.runLater(()-> {
                if (resolution == ResolutionState.FAILED) {
                    callback.apply(failMsg);
                } else {
                    // Resubmit the callback? Probably not, this means we momentarily were in the correct state, but
                    // if we return to it, things could get weird
//                    onFailed(callback);
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
     * Checks if the current state can be used as a root to turn into the state
     * described by the supplied arguments.
     *
     * While it is technically valid to interleave filters and sorts,
     * for maximal performance, we want to ensure filters are always applied
     * before sorts; so, if you have added a filter, but the current state
     * has any sorts at all, we will return false, even though this
     * state would be correct when used, it won't be maximally efficient.
     *
     * Additionally, custom columns might be used in filters and sorts,
     * but a filter or sort on a custom column cannot be created until
     * that custom column has been exported and provides a table definition
     * so that we can supply Column instances to operate upon.
     *
     * @param sorts The full set of sorts we want in our resulting table state
     * @param filters The full set of filters we want in our resulting table state
     * @param customColumns The full set of custom columns we want in our resulting table state
     * @param flat True if the resulting table should have a flatten index, false otherwise
     * @return true if this state can be used to add
     */
    public boolean isCompatible(List<Sort> sorts, List<FilterCondition> filters, List<CustomColumnDescriptor> customColumns, boolean flat) {
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

        if (isFlat() != flat) {
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
     * @return true if there are no tables bound to this state.
     *
     * If a table that had a subscription for this state was orphaned by a pending request,
     * we want to clear the subscription immediately so it becomes inert (immediately remove the subscription),
     * but we may need to rollback the request, and we don't want to release the
     * handle until the pending request is finished (whereupon we will remove the binding).
     */
    public boolean isEmpty() {
        return active.size == 0 && paused.size == 0 && retainers.size == 0;
    }

    /**
     * Call this with a marker object that you want to use to cause the state to be retained.
     * You either need to hold onto the returned function to clear your binding, or call
     * {@link #unretain(Object)} when finished to avoid keeping this handle alive beyond its
     * usefulness.
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

    public boolean isActiveEmpty() {
        return active.size == 0;
    }

    public boolean hasNoSubscriptions() {
        return JsItr.iterate(active.values()).allMatch(binding -> binding.getSubscription() == null);
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
        return resolution == ResolutionState.RUNNING || resolution == ResolutionState.FAILED || resolution == ResolutionState.RELEASED;
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

    public void setDesiredViewport(JsTable table, long firstRow, long lastRow, Column[] columns) {
        touch();
        final ActiveTableBinding sub = active.get(table);
        assert sub != null : "You cannot set the desired viewport on a non-active state + table combination";
        final RangeSet rows = sub.setDesiredViewport(firstRow, lastRow, columns);
        // let event loop eat multiple viewport sets and only apply the last one (winner of who gets spot in map)
        LazyPromise.runLater(()->{
            if (sub.getRows() == rows) {
                // winner! now, on to the next hurdle... ensuring we have columns.
                // TODO: have an onColumnsReady callback, for cases when we know we're only waiting on non-column-modifying operations
                onRunning(self->{
                    if (sub.getRows() == rows) {
                        // winner again!
                        applyViewport(sub);
                    }
                }, JsRunnable.doNothing());
            }
        });
    }

    public void subscribe(JsTable table, Column[] columns) {
        touch();
        ActiveTableBinding binding = active.get(table);
        assert binding != null : "No active binding found for table " + table;

        onRunning(self -> {
            binding.setSubscriptionPending(true);

            if (getHandle().equals(table.getHandle())) {
                binding.setViewport(new Viewport(null, makeBitset(columns)));
                table.getConnection().scheduleCheck(this);
            }
        }, JsRunnable.doNothing());
    }

    private void applyViewport(ActiveTableBinding sub) {
        sub.setSubscriptionPending(false);
        final JsTable table = sub.getTable();
        // make sure we're still the tail entry before trying to apply viewport
        assert isRunning() : "Do not call this method unless you are in a running state! " + this;
        if (getHandle().equals(table.getHandle())) {
            final RangeSet rows = sub.getRows();
            Column[] desired = sub.getColumns();
            if (Js.isFalsy(desired)) {
                desired = getColumns();
            }
            Viewport vp = new Viewport(rows, makeBitset(desired));
            sub.setViewport(vp);
            table.refreshViewport(this, vp);
        }
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
            + "active: " +active +" paused: " + paused;
        return iterate(active.keys()).plus(iterate(paused.keys()));
    }

    public void forActiveSubscriptions(JsBiConsumer<JsTable, Viewport> callback) {
        JsItr.forEach(active, (table, binding)-> {
            if (binding.getSubscription() != null) {
                assert binding.getTable() == table : "Corrupt binding between " + table + " and " + binding + " in " + active;
                callback.apply((JsTable)table, binding.getSubscription());
            }
        });
    }

    public void forActiveTables(JsConsumer<JsTable> callback) {
        JsItr.forEach(active, (table, sub)-> {
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

    public void handleDelta(DeltaUpdates updates) {
        assert size != SIZE_UNINITIALIZED : "Received delta before receiving initial size";
        setSize(size + updates.getAdded().size() - updates.getRemoved().size());
        forActiveSubscriptions((table, subscription)->{
            assert table.getHandle().equals(handle);
            // we are the active state of this table, so forward along the delta.
            table.handleDelta(this, updates);
        });
    }

    public void setSubscribed(boolean subscribed) {
        this.subscribed = subscribed;
    }

    public boolean isSubscribed() {
        return subscribed;
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
        return existing == null ? null : existing.isActive() ? (ActiveTableBinding) existing : ((PausedTableBinding)existing).getActiveBinding();
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
        // Now, we want to put back the viewport, if any.
        refreshSubscription(was.getActiveBinding());
    }

    private void refreshSubscription(ActiveTableBinding sub) {
        assert active.get(sub.getTable()) == sub;
        if (!sub.isSubscriptionPending()) {
            sub.maybeReviveSubscription();
        }
    }

    public MappedIterable<ClientTableState> ancestors() {
        return new LinkedIterable<>(this, ClientTableState::getPrevious, true);
    }

    public MappedIterable<ClientTableState> reversed() {
        return new LinkedIterable<>(this, ClientTableState::getPrevious, false);
    }

    /**
     * Look through paused tables to see if any of them have been
     */
    public void cleanup() {
        assert JsItr.iterate(active.keys()).allMatch(t->!t.isAlive() || t.state() == this) : "active map not up-to-date with tables";
        if (getResolution() == ResolutionState.RELEASED) {
            for (PausedTableBinding sub : JsItr.iterate(paused.values())) {
                releaseTable(sub.getActiveBinding().getTable());
            }
            active.clear();
            // notify any retainers who have events that we've been released.
            for (Object retainer : JsItr.iterate(retainers.values())) {
                if (retainer instanceof HasEventHandling) {
                    ((HasEventHandling)retainer).fireEventWithDetail(HasEventHandling.INTERNAL_EVENT_RELEASED, this);
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
                // release the table if it no longer has a reference to the handle with any of it's bindings, including paused bindings
                // don't releaseTable for tables that are in flux
                if (table.state().isRunning()) {
                    releaseTable(table);
                } else {
                    // wait until the table's current state is complete to try cleaning up again.
                    table.state().onRunning(i->cleanup(), this::cleanup);
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
        return (a, b)->(int)Math.signum(b.getLastTouched() - a.getLastTouched());
    }

    public Promise<JsTable> fetchTable(HasEventHandling failHandler, BrowserHeaders metadata) {
        return refetch(failHandler, metadata).then(cts->Promise.resolve(new JsTable(connection, cts)));
    }

    public Promise<ClientTableState> refetch(HasEventHandling failHandler, BrowserHeaders metadata) {
        final Promise<ExportedTableCreationResponse> promise = Callbacks.grpcUnaryPromise(c ->
            fetch.fetch(c, this, metadata)
        );
        //noinspection unchecked
        return promise.then(def -> {
            if (resolution == ResolutionState.RELEASED) {
                // was released before we managed to finish the fetch, ignore
                //noinspection rawtypes,unchecked
                return (Promise) Promise.reject("Table already released, cannot process incoming table definition, this can be safely ignored.");
            }
            applyTableCreationResponse(def);
            return Promise.resolve(this);
        });
    }

    public void applyTableCreationResponse(ExportedTableCreationResponse def) {
        // by definition, the ticket is now exported and connected
        handle.setState(TableTicket.State.EXPORTED);
        handle.setConnected(true);

        // we conform to flight's schema representation of:
        //  - IPC_CONTINUATION_TOKEN (4-byte int of -1)
        //  - message size (4-byte int)
        //  - a Message wrapping the schema
        ByteBuffer bb = new ByteBuffer(def.getSchemaHeader_asU8());
        bb.setPosition(bb.position() + 8);
        Message headerMessage = Message.getRootAsMessage(bb);

        assert headerMessage.headerType() == MessageHeader.Schema;
        Schema schema = headerMessage.header(new Schema());

        ColumnDefinition[] cols = new ColumnDefinition[(int)schema.fieldsLength()];
        for (int i = 0; i < schema.fieldsLength(); i++) {
            cols[i] = new ColumnDefinition();
            Field f = schema.fields(i);
            Map<String, String> fieldMetadata = new HashMap<>();
            for (int j = 0; j < f.customMetadataLength(); j++) {
                KeyValue keyValue = f.customMetadata(j);
                fieldMetadata.put(keyValue.key().asString(), keyValue.value().asString());
            }
            cols[i].setName(f.name().asString());
            cols[i].setColumnIndex(i);
            cols[i].setType(fieldMetadata.get("deephaven:type"));
            cols[i].setStyleColumn("true".equals(fieldMetadata.get("deephaven:isStyle")));
            cols[i].setFormatColumn("true".equals(fieldMetadata.get("deephaven:isDateFormat")) || "true".equals(fieldMetadata.get("deephaven:isNumberFormat")));
            cols[i].setForRow("true".equals(fieldMetadata.get("deephaven:isRowStyle")));

            String formatColumnName = fieldMetadata.get("deephaven:dateFormatColumn");
            if (formatColumnName == null) {
                formatColumnName = fieldMetadata.get("deephaven:numberFormatColumn");
            }
            cols[i].setFormatColumnName(formatColumnName);

            cols[i].setStyleColumnName(fieldMetadata.get("deephaven:styleColumn"));
        }

        TableAttributesDefinition attributes = new TableAttributesDefinition();
        attributes.setValues(new String[0]);
        attributes.setKeys(new String[0]);
        attributes.setRemainingKeys(new String[0]);
        setTableDef(new InitialTableDefinition()
                .setAttributes(attributes)
                .setColumns(cols)
                .setFlat(false)
                .setId(null)
                .setSize(Long.parseLong(def.getSize()))
        );

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
}
