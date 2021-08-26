package io.deephaven.db.util;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListenerAdapter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.TerminalNotification;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * Listens to a table, mapping keys to values.
 *
 * When you call get, we return the value as of the start of this clock cycle.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ToMapListener<K, V> extends InstrumentedShiftAwareListenerAdapter implements Map<K, V> {
    private static final long NO_ENTRY_VALUE = -2;
    private static final long DELETED_ENTRY_VALUE = -1;

    private final TObjectLongHashMap<Object> baselineMap = new TObjectLongHashMap<>(8, 0.5f, NO_ENTRY_VALUE);
    private volatile TObjectLongHashMap<Object> currentMap;

    private final LongFunction<K> keyProducer;
    private final LongFunction<K> prevKeyProducer;
    private final LongFunction<V> valueProducer;
    private final LongFunction<V> prevValueProducer;

    public static ToMapListener make(DynamicTable source, String keySourceName) {
        return QueryPerformanceRecorder.withNugget("ToMapListener(" + keySourceName + ")",
                () -> new ToMapListener(source, keySourceName, keySourceName));
    }

    public static ToMapListener make(DynamicTable source, String keySourceName, String valueSourceName) {
        return QueryPerformanceRecorder.withNugget("ToMapListener(" + keySourceName + ", " + valueSourceName + ")",
                () -> new ToMapListener(source, keySourceName, valueSourceName));
    }

    public static <K1, V1> ToMapListener<K1, V1> make(DynamicTable source, ColumnSource<K1> keySource,
            ColumnSource<V1> valueSource) {
        // noinspection unchecked
        return QueryPerformanceRecorder.withNugget("ToMapListener",
                () -> new ToMapListener<>(source, keySource, valueSource));
    }

    public static <K1, V1> ToMapListener<K1, V1> make(DynamicTable source, LongFunction<K1> keyProducer,
            LongFunction<K1> prevKeyProducer, LongFunction<V1> valueProducer, LongFunction<V1> prevValueProducer) {
        // noinspection unchecked
        return QueryPerformanceRecorder.withNugget("ToMapListener",
                () -> new ToMapListener<>(source, keyProducer, prevKeyProducer, valueProducer, prevValueProducer));
    }

    private ToMapListener(DynamicTable source, String keySourceName, String valueSourceName) {
        // noinspection unchecked
        this(source, source.getColumnSource(keySourceName), source.getColumnSource(valueSourceName));
    }

    private ToMapListener(DynamicTable source, ColumnSource<K> keySource, ColumnSource<V> valueSource) {
        this(source, keySource::get, keySource::getPrev, valueSource::get, valueSource::getPrev);
    }

    private ToMapListener(DynamicTable source, LongFunction<K> keyProducer, LongFunction<K> prevKeyProducer,
            LongFunction<V> valueProducer, LongFunction<V> prevValueProducer) {
        super(source, false);
        this.keyProducer = keyProducer;
        this.prevKeyProducer = prevKeyProducer;
        this.valueProducer = valueProducer;
        this.prevValueProducer = prevValueProducer;

        for (final Index.Iterator it = source.getIndex().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            baselineMap.put(keyProducer.apply(key), key);
        }
    }

    @Override
    public void onUpdate(final Update upstream) {
        final int cap = upstream.added.intSize() + upstream.removed.intSize() + upstream.modified.intSize();
        final TObjectLongHashMap<Object> newMap = new TObjectLongHashMap<>(cap, 0.5f, NO_ENTRY_VALUE);

        final LongConsumer remover = (final long key) -> {
            newMap.put(prevKeyProducer.apply(key), DELETED_ENTRY_VALUE);
        };
        upstream.removed.forAllLongs(remover);
        upstream.getModifiedPreShift().forAllLongs(remover);

        final LongConsumer adder = (final long key) -> {
            newMap.put(keyProducer.apply(key), key);
        };
        upstream.added.forAllLongs(adder);
        upstream.modified.forAllLongs(adder);

        currentMap = newMap;
        LiveTableMonitor.DEFAULT.addNotification(new Flusher());
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

    public <T> T get(K key, groovy.lang.Closure<T> valueProducer, groovy.lang.Closure<T> prevValueProducer) {
        return get(key, (long row) -> (T) valueProducer.call(row), (long row) -> (T) prevValueProducer.call(row));
    }

    public <T> T get(K key, ColumnSource<T> cs) {
        return get(key, cs::get, cs::getPrev);
    }

    /**
     * Get but instead of applying the default value producer, use a custom value producer.
     *
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
        final LogicalClock.State state = LogicalClock.DEFAULT.currentState();
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
    public Set<Entry<K, V>> entrySet() {
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
}
