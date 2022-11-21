/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
abstract class HierarchicalTableImpl<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends HierarchicalTableImpl<IFACE_TYPE, IMPL_TYPE>>
        extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>
        implements HierarchicalTable<IFACE_TYPE> {

    /**
     * The source table that operations were applied to in order to produce this hierarchical table.
     */
    final QueryTable source;

    /**
     * The root node of the hierarchy.
     */
    final QueryTable root;

    protected HierarchicalTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final QueryTable root) {
        super(initialAttributes);
        this.source = source;
        this.root = root;
    }

    @Override
    public Table getSource() {
        return source;
    }

    @Override
    public Table getRoot() {
        return root;
    }

    @Override
    public SnapshotState makeSnapshotState() {
        return null;
    }

    IFACE_TYPE noopResult() {
        if (getSource().isRefreshing()) {
            manageWithCurrentScope();
        }
        // noinspection unchecked
        return (IFACE_TYPE) this;
    }

    @Override
    protected void checkAvailableColumns(@NotNull final Collection<String> columns) {
        final Set<String> availableColumns = root.getDefinition().getColumnNameMap().keySet();
        final List<String> missingColumns =
                columns.stream().filter(column -> !availableColumns.contains(column)).collect(Collectors.toList());
        if (!missingColumns.isEmpty()) {
            throw new NoSuchColumnException(availableColumns, missingColumns);
        }
    }

    static final class SnapshotStateImpl implements SnapshotState {

        private final Map<Table, KeyTableState> cachedKeyTableStates = new WeakHashMap<>();
        private final KeyedLongObjectHashMap<NodeState> cachedNodes = new KeyedLongObjectHashMap<>(NodeState.ID_KEY);

        KeyTableState getKeyTableState(
                @NotNull Table keyTable,
                @NotNull ColumnName keyTableExpandDescendantsColumn,
                @NotNull final BiFunction<Table, ColumnName, ? extends KeyTableState> keyTableStateFactory) {
            return cachedKeyTableStates.computeIfAbsent(keyTable,
                    kt -> keyTableStateFactory.apply(keyTable, keyTableExpandDescendantsColumn));
        }

        NodeState getNodeState(final long id, @NotNull final LongFunction<NodeState> nodeStateFactory) {
            return cachedNodes.putIfAbsent(id, nodeStateFactory::apply);
        }

        @Override
        public void close() {
            cachedNodes.forEach(NodeState::release);
            cachedNodes.clear();
        }
    }

    /**
     * Opaque per-type key table state used in computing snapshots.
     */
    interface KeyTableState {
    }

    static final class NodeState {

        private static final AtomicIntegerFieldUpdater<NodeState> RETENTION_COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(NodeState.class, "retentionCount");
        private static final KeyedLongObjectKey<NodeState> ID_KEY = new KeyedLongObjectKey.BasicStrict<>() {
            @Override
            public long getLongKey(@NotNull final NodeState nodeState) {
                return nodeState.id;
            }
        };

        /**
         * Node identifier, a type-specific identifier that uniquely maps to a single Table node in the
         * HierarchicalTable.
         */
        private final long id;

        /**
         * The Table at this node, after any node-level operations have been applied.
         */
        private final Table processed;

        /**
         * The sort {@link RowRedirection} that maps the appropriate unsorted row key space to {@link #processed
         * processed's} outer row space.
         */
        private final RowRedirection sortRedirection;

        private volatile int retentionCount;

        NodeState(final long id, final Table processed, final boolean needsRedirection) {
            this.id = id;
            this.processed = processed;
            if (processed.isRefreshing()) {
                RETENTION_COUNT_UPDATER.set(this, 1);
                processed.retainReference();
            }
            sortRedirection = needsRedirection ? SortOperation.getRowRedirection(processed) : null;
        }

        Table getProcessed() {
            return processed;
        }

        RowRedirection getSortRedirection() {
            return sortRedirection;
        }

        private void release() {
            if (RETENTION_COUNT_UPDATER.compareAndSet(this, 1, 0)) {
                processed.dropReference();
            }
        }
    }
    // TODO-RWC: Be sure to take format columns into account for table definitions. Prune formats applied to both from
    // UI.
}
