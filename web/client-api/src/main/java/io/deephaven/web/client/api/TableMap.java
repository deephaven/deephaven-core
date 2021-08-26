package io.deephaven.web.client.api;

import elemental2.core.JsObject;
import elemental2.core.JsSet;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.TableMapDeclaration;
import io.deephaven.web.shared.data.TableMapHandle;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@JsType(namespace = "dh")
public class TableMap extends HasEventHandling {
    private static final class LocalKey {
        private final String str;
        private final String[] arr;

        protected static LocalKey of(final Object key) {
            if (key instanceof String) {
                return new LocalKey((String) key);
            } else {
                JsObject.freeze(key);
                return new LocalKey((String[]) key);
            }
        }

        public LocalKey(final String str) {
            this.str = str;
            this.arr = null;
        }

        public LocalKey(final String[] arr) {
            this.arr = arr;
            this.str = null;
        }

        public Object unwrap() {
            if (arr != null) {
                return arr;
            }
            return str;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final LocalKey localKey = (LocalKey) o;

            if (str != null ? !str.equals(localKey.str) : localKey.str != null)
                return false;
            return Arrays.equals(arr, localKey.arr);
        }

        @Override
        public int hashCode() {
            if (str != null) {
                assert arr == null;
                return str.hashCode();
            }
            return Arrays.hashCode(arr);
        }
    }

    public static final String EVENT_KEYADDED = "keyadded",
        EVENT_DISCONNECT = JsTable.EVENT_DISCONNECT,
        EVENT_RECONNECT = JsTable.EVENT_RECONNECT,
        EVENT_RECONNECTFAILED = JsTable.EVENT_RECONNECTFAILED;

    private final WorkerConnection workerConnection;
    private final Consumer<Callback<TableMapDeclaration, String>> fetch;

    private TableMapHandle tableMapHandle;

    // Represents the sorta-kinda memoized results, tables that we've already locally fetched from
    // the tablemap,
    // and if all references to a table are released, entries here will be replaced with unresolved
    // instances so
    // we don't leak server references or memory.
    private final Map<LocalKey, JsLazy<Promise<ClientTableState>>> tables = new HashMap<>();

    @JsIgnore
    public TableMap(WorkerConnection workerConnection, String tableMapName) {
        this.workerConnection = workerConnection;
        this.fetch = c -> {
            // workerConnection.getServer().fetchTableMap(tableMapName, c);
            throw new UnsupportedOperationException("fetchTableMap");
        };
    }

    @JsIgnore
    public TableMap(WorkerConnection workerConnection,
        Consumer<Callback<TableMapDeclaration, String>> fetch) {
        this.workerConnection = workerConnection;
        this.fetch = fetch;
    }

    /**
     * Fetches (or re-fetches) the TableMap so it can be used internally
     */
    @JsIgnore
    public Promise<TableMap> refetch() {
        return Callbacks.promise(this, fetch).then(decl -> {
            workerConnection.registerTableMap(decl.getHandle(), this);
            tableMapHandle = decl.getHandle();
            for (Object key : (Object[]) decl.getKeys().getData()) {
                LocalKey k = LocalKey.of(key);
                put(key, k);
            }
            unsuppressEvents();
            fireEvent(EVENT_RECONNECT);
            return Promise.resolve(this);
        }).catch_(err -> {
            final CustomEventInit init = CustomEventInit.create();
            init.setDetail(err);
            unsuppressEvents();
            fireEvent(EVENT_RECONNECTFAILED, init);
            suppressEvents();

            // noinspection unchecked
            return (Promise<TableMap>) (Promise) Promise.reject(err);
        });
    }

    public Promise<JsTable> getTable(Object key) {
        // Every caller gets a fresh table instance, and when all are closed, the CTS will be
        // released.
        // See #put for how that is tracked.
        final JsLazy<Promise<ClientTableState>> entry = tables.get(LocalKey.of(key));
        if (entry == null) {
            // key doesn't even exist, just hand back a null table
            return Promise.resolve((JsTable) null);
        }
        return entry.get().then(cts -> Promise.resolve(new JsTable(cts.getConnection(), cts)));
    }

    public Promise<JsTable> getMergedTable() {
        return workerConnection.newState((c, cts, metadata) -> {
            // workerConnection.getServer().getMergedTableMap(tableMapHandle, cts.getHandle(), c);
            throw new UnsupportedOperationException("getMergedTableMap");
        }, "tablemap merged table")
            .refetch(this, workerConnection.metadata())
            .then(cts -> Promise.resolve(new JsTable(cts.getConnection(), cts)));
    }

    public JsSet<Object> getKeys() {
        return new JsSet<>(tables.keySet().stream().map(LocalKey::unwrap).toArray());
    }

    @JsProperty(name = "size")
    public int size() {
        return tables.size();
    }

    public void close() {
        workerConnection.releaseTableMap(this, tableMapHandle);
    }

    @JsIgnore
    public void notifyKeyAdded(Object key) {
        LocalKey k = LocalKey.of(key);

        if (!tables.containsKey(k)) {
            put(key, k);

            CustomEventInit init = CustomEventInit.create();
            init.setDetail(key);
            fireEvent(EVENT_KEYADDED, init);
        }
    }

    protected void put(Object key, LocalKey localKey) {
        tables.put(localKey, JsLazy.of(() -> {
            // If we've entered this lambda, the JsLazy is being used, so we need to go ahead and
            // get the tablehandle
            final ClientTableState entry = workerConnection.newState((c, cts, metadata) -> {
                // if (key == null || key instanceof String) {
                // workerConnection.getServer().getTableMapStringEntry(tableMapHandle,
                // cts.getHandle(), (String) key, c);
                // } else {
                // workerConnection.getServer().getTableMapStringArrayEntry(tableMapHandle,
                // cts.getHandle(), (String[]) key, c);
                // }
                throw new UnsupportedOperationException("getTableMapEntry");
            },
                "tablemap key " + key);

            // later, when the CTS is released, remove this "table" from the map and replace with an
            // unresolved JsLazy
            entry.onRunning(ignore -> {
            }, ignore -> {
            }, () -> put(key, localKey));

            // we'll make a table to return later, this func here just produces the JsLazy of the
            // CTS
            return entry.refetch(this, workerConnection.metadata());
        }));
    }
}
