/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.hash.KeyedIntObjectHash;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.hash.KeyedObjectHash;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class manages instances of client -> set
 * <table>
 * because hierarchical tables will apply sorting/filtering to each individual table. Instead of serializing and holding
 * these references on the client, we will retain and manage their lifecycle here.
 */
@SuppressWarnings("rawtypes")
public enum TreeTableClientTableManager {
    DEFAULT;

    // TODO (deephaven/deephaven-core/issues/37): Refine this type into something useful, or refactor entirely.
    public interface Client {
        void addDisconnectHandler(@NotNull Consumer<Client> handler);

        void removeDisconnectHandler(@NotNull Consumer<Client> handler);
    }

    private static final class ClientStateKey extends KeyedObjectKey.Exact<Client, ClientState> {

        @Override
        public Client getKey(@NotNull final ClientState clientState) {
            return clientState.client;
        }
    }

    private static final class ViewportStateKey extends KeyedIntObjectKey.BasicStrict<TreeState> {
        @Override
        public int getIntKey(TreeState viewportState) {
            return viewportState.rootTableId;
        }
    }

    private final Consumer<Client> DISCONNECT_HANDLER = this::release;
    private final KeyedObjectHash<Client, ClientState> statesByClient = new KeyedObjectHash<>(new ClientStateKey());

    /**
     * The complete state of all trees for a particular client.
     */
    public static class ClientState {
        // TODO-RWC:
        // 1. This becomes the holder for all "properties" of the snapshot
        // Includes: hierarchical table (Ticket), key table (Ticket), filters, sorts
        // 2. Snapshots should be applied to a Ticket representing an exported ClientState, not the Table directly.
        // 3. ClientState + TreeState becomes one object

        final Client client;

        /** Map the original root table id, to the list of all child tables to retain */
        final KeyedIntObjectHash<TreeState> retentionMap = new KeyedIntObjectHash<>(new ViewportStateKey());

        ClientState(Client clientId) {
            this.client = clientId;
        }

        public void release(final int baseTableId) {
            final TreeState state = retentionMap.removeKey(baseTableId);
            state.releaseAll();
        }

        synchronized void releaseAll() {
            retentionMap.values().forEach(TreeState::releaseAll);
            retentionMap.clear();
        }

        TreeState getTreeState(int baseTableId, Supplier<SnapshotState> userStateFactory) {
            final KeyedIntObjectHash.ValueFactory<TreeState> integerTreeStateValueFactory =
                    new KeyedIntObjectHash.ValueFactory.Strict<TreeState>() {
                        @Override
                        public TreeState newValue(int key) {
                            return new TreeState(key, userStateFactory.get());
                        }
                    };

            return retentionMap.putIfAbsent(baseTableId, integerTreeStateValueFactory);
        }
    }

    /**
     * The state of a single hierarchical table. This includes a unique {@link TableState} for each expanded row of the
     * table.
     */
    public static class TreeState {
        final Map<Object, TableState> expandedTables = new HashMap<>();
        final int rootTableId;
        final SnapshotState userState;

        TreeState(int rootTableId, SnapshotState userState) {
            this.rootTableId = rootTableId;
            this.userState = userState;
        }

        synchronized void releaseAll() {
            expandedTables.values().forEach(TableState::release);
            expandedTables.clear();
        }

        synchronized void retain(Object key, Table table) {
            expandedTables.computeIfAbsent(key, k -> new TableState(table));
        }

        synchronized void releaseIf(Predicate<Object> test) {
            for (final Iterator<Map.Entry<Object, TableState>> entryIterator =
                    expandedTables.entrySet().iterator(); entryIterator.hasNext();) {
                final Map.Entry<Object, TableState> entry = entryIterator.next();
                if (test.test(entry.getKey())) {
                    entry.getValue().release();
                    entryIterator.remove();
                }
            }
        }

        public synchronized Table getTable(Object key) {
            final TableState state = expandedTables.get(key);
            return state == null ? null : state.getTable();
        }

        SnapshotState getUserState() {
            return userState;
        }
    }

    /**
     * Liveness holder for a single table within a hierarchy.
     */
    static class TableState extends SingletonLivenessManager {
        private final Table table;

        private TableState(@NotNull final Table table) {
            this.table = table;
            if (table.isRefreshing()) {
                manage(table);
            }
        }

        Table getTable() {
            return table;
        }
    }

    /**
     * Get the client state for a specific client.
     *
     * @param client the client to retrieve state for
     * @return the {@link ClientState client state} for the specified client
     */
    public ClientState get(Client client) {
        // Note that putIfAbsent(K, Factory<V>) is distinctly different in behavior from putIfAbsent(K,V). It will
        // return
        // the new value, or the existing value, it will not return null like putIfAbsent(K,V).
        return statesByClient.putIfAbsent(client, clt -> {
            // noinspection unchecked
            client.addDisconnectHandler(DISCONNECT_HANDLER);
            return new ClientState(clt);
        });
    }


    public void release(Client client) {
        final ClientState state = statesByClient.removeKey(client);

        if (state != null) {
            state.releaseAll();
            // noinspection unchecked
            client.removeDisconnectHandler(DISCONNECT_HANDLER);
        }
    }
}
