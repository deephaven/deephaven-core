//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.TerminalNotification;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * Listens to a table, mapping keys to values.
 * <p>
 * When you call get, we return the value as of the start of this clock cycle.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ToMapListener<K, V> extends InstrumentedTableUpdateListenerAdapter implements Map<K, V> {
    private static final long NO_ENTRY_VALUE = -2;
    private static final long DELETED_ENTRY_VALUE = -1;

    private final TObjectLongHashMap<Object> baselineMap = new TObjectLongHashMap<>(8, 0.5f, NO_ENTRY_VALUE);
    private volatile TObjectLongHashMap<Object> currentMap;

    private final LongFunction<K> keyProducer;
    private final LongFunction<K> prevKeyProducer;
    private final LongFunction<V> valueProducer;
    private final LongFunction<V> prevValueProducer;

    public static ToMapListener make(Table source, String keySourceName) {
        return QueryPerformanceRecorder.withNugget("ToMapListener(" + keySourceName + ")",
                () -> new ToMapListener(source, keySourceName, keySourceName));
    }

    public static ToMapListener make(Table source, String keySourceName, String valueSourceName) {
        return QueryPerformanceRecorder.withNugget("ToMapListener(" + keySourceName + ", " + valueSourceName + ")",
                () -> new ToMapListener(source, keySourceName, valueSourceName));
    }

    public static <K1, V1> ToMapListener<K1, V1> make(Table source, ColumnSource<K1> keySource,
            ColumnSource<V1> valueSource) {
        return QueryPerformanceRecorder.withNugget("ToMapListener",
                () -> new ToMapListener<>(source, keySource, valueSource));
    }

    public static <K1, V1> ToMapListener<K1, V1> make(Table source, LongFunction<K1> keyProducer,
            LongFunction<K1> prevKeyProducer, LongFunction<V1> valueProducer, LongFunction<V1> prevValueProducer) {
        return QueryPerformanceRecorder.withNugget("ToMapListener",
                () -> new ToMapListener<>(source, keyProducer, prevKeyProducer, valueProducer, prevValueProducer));
    }

    private ToMapListener(Table source, String keySourceName, String valueSourceName) {
        this(source, source.getColumnSource(keySourceName), source.getColumnSource(valueSourceName));
    }

    private ToMapListener(Table source, ColumnSource<K> keySource, ColumnSource<V> valueSource) {
        this(source, keySource::get, keySource::getPrev, valueSource::get, valueSource::getPrev);
    }

    private ToMapListener(Table source, LongFunction<K> keyProducer, LongFunction<K> prevKeyProducer,
            LongFunction<V> valueProducer, LongFunction<V> prevValueProducer) {
        super(source, false);
        this.keyProducer = keyProducer;
        this.prevKeyProducer = prevKeyProducer;
        this.valueProducer = valueProducer;
        this.prevValueProducer = prevValueProducer;

        for (final RowSet.Iterator it = source.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            baselineMap.put(keyProducer.apply(key), key);
        }
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        final int cap = upstream.added().intSize() + upstream.removed().intSize() + upstream.modified().intSize();
        final TObjectLongHashMap<Object> newMap = new TObjectLongHashMap<>(cap, 0.5f, NO_ENTRY_VALUE);

        final LongConsumer remover = (final long key) -> {
            newMap.put(prevKeyProducer.apply(key), DELETED_ENTRY_VALUE);
        };
        upstream.removed().forAllRowKeys(remover);
        upstream.getModifiedPreShift().forAllRowKeys(remover);

        final LongConsumer adder = (final long key) -> {
            newMap.put(keyProducer.apply(key), key);
        };
        upstream.added().forAllRowKeys(adder);
        upstream.modified().forAllRowKeys(adder);

        currentMap = newMap;
        getUpdateGraph().addNotification(new Flusher());
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        // noinspection unchecked
        return get((K) key, valueProducer, prevValueProducer);
    }

    public <T> T get(K key, ColumnSource<T> cs) {
        return get(key, cs::get, cs::getPrev);
    }

    @NotNull
    public RowSetForKeysResult getRowSetForKeys(List<K> dataKeys) {
        final LogicalClock.State state = getUpdateGraph().clock().currentState();
        TObjectLongHashMap<Object> map;

        final boolean useBaselineMap;
        if (state == LogicalClock.State.Idle && (map = currentMap) != null) {
            useBaselineMap = false;
        } else {
            map = baselineMap;
            useBaselineMap = true;
        }

        final Pair<RowSet, TLongIntMap> result;
        if (useBaselineMap) {
            Assert.eq(map, "map", baselineMap, "baselineMap");
            synchronized (baselineMap) {
                result = getRowSetForKeys0(dataKeys, map);
            }
        } else {
            result = getRowSetForKeys0(dataKeys, map);
        }

        return new RowSetForKeysResult(result.first, result.second, state == LogicalClock.State.Updating);
    }

    @NotNull
    private Pair<RowSet, TLongIntMap> getRowSetForKeys0(List<K> dataKeys, TObjectLongHashMap<Object> map) {
        final int nKeys = dataKeys.size();
        final TLongIntMap idxKeyToDataKeyPositionMap = new TLongIntHashMap(nKeys);
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        int ii = 0;
        for (K dataKey : dataKeys) {
            long k = map.get(dataKey);
            if (k != NO_ENTRY_VALUE && k != DELETED_ENTRY_VALUE) {
                builder.addKey(k);
                idxKeyToDataKeyPositionMap.put(k, ii);
            }

            ii++;
        }

        return new Pair<>(builder.build(), idxKeyToDataKeyPositionMap);
    }

    public Table getSource() {
        return super.source;
    }

    /**
     * Get but instead of applying the default value producer, use a custom value producer.
     * <p>
     * The intention is that you can wrap the map up with several different value producers, e.g. one for bid and
     * another for ask.
     *
     * @param key the key to retrieve
     * @param valueProducer retrieve the current value out of the table
     * @param prevValueProducer retrieve the previous value out of the table
     * @param <T> the type of the value we are retrieving
     *
     * @return the value associated with key
     */
    public <T> T get(K key, LongFunction<T> valueProducer, LongFunction<T> prevValueProducer) {
        final LogicalClock.State state = getUpdateGraph().clock().currentState();
        final TObjectLongHashMap<Object> map;
        if (state == LogicalClock.State.Idle && (map = currentMap) != null) {
            final long row = map.get(key);
            if (row != map.getNoEntryValue()) {
                if (row == DELETED_ENTRY_VALUE) {
                    return null;
                }
                return valueProducer.apply(row);
            }
        }

        final long row;
        synchronized (baselineMap) {
            row = baselineMap.get(key);
            if (row == baselineMap.getNoEntryValue()) {
                return null;
            }
        }
        return state == LogicalClock.State.Updating ? prevValueProducer.apply(row) : valueProducer.apply(row);
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    private class Flusher extends TerminalNotification {
        @Override
        public void run() {
            synchronized (baselineMap) {
                baselineMap.putAll(currentMap);
            }
            currentMap = null;
        }
    }

    public static class RowSetForKeysResult {
        public final RowSet rowSet;
        public final TLongIntMap idxKeyToDataKeyPositionMap;
        public final boolean usePrev;

        private RowSetForKeysResult(RowSet rowSet, TLongIntMap idxKeyToDataKeyPositionMap, boolean usePrev) {
            this.rowSet = rowSet;
            this.idxKeyToDataKeyPositionMap = idxKeyToDataKeyPositionMap;
            this.usePrev = usePrev;
        }
    }
}
