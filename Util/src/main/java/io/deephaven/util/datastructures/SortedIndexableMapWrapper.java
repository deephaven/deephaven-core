package io.deephaven.util.datastructures;

import io.deephaven.hash.IndexableMap;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This is a simple utility class that wraps a map, and presents it as an IndexableMap.
 *
 * The getByIndex() and values() method sort the values according the passed in Comparator.
 *
 * The other access methods (iteration, keySet etc. do not sort the results).
 *
 * Everything that touches the values list should be synchronized.
 */
public class SortedIndexableMapWrapper<K, V> implements IndexableMap<K, V> {
    private final Map<K, V> baseMap;
    private final Comparator<V> comparator;

    // cached copy of the values, wiped on any modification, created by getByIndex
    private ArrayList<Map.Entry<K, V>> valueList;

    public SortedIndexableMapWrapper(Map<K, V> baseMap, Comparator<V> comparator) {
        this.baseMap = baseMap;
        this.comparator = comparator;
        this.valueList = null;
    }

    // on any modification, we wipe the valueList
    private synchronized void clearList() {
        this.valueList = null;
    }

    // get the sorted values list, returning the cached version if available
    private synchronized List<Map.Entry<K, V>> getValuesList() {
        if (valueList != null) {
            return valueList;
        }
        valueList = baseMap.entrySet().stream()
            .sorted((e1, e2) -> comparator.compare(e1.getValue(), e2.getValue()))
            .collect(Collectors.toCollection(ArrayList::new));
        return valueList;
    }

    @Override
    public int size() {
        return baseMap.size();
    }

    @Override
    public boolean isEmpty() {
        return baseMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return baseMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return baseMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return baseMap.get(key);
    }

    @Override
    public synchronized V put(K key, V value) {
        clearList();
        return baseMap.put(key, value);
    }

    @Override
    public synchronized V remove(Object key) {
        clearList();
        return baseMap.remove(key);
    }

    @Override
    public synchronized void putAll(@NotNull Map<? extends K, ? extends V> m) {
        clearList();
        baseMap.putAll(m);
    }

    @Override
    public synchronized void clear() {
        clearList();
        baseMap.clear();
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return baseMap.keySet();
    }

    @NotNull
    @Override
    public synchronized Collection<V> values() {
        return Collections.unmodifiableList(
            getValuesList().stream().map(Entry::getValue).collect(Collectors.toList()));
    }

    @NotNull
    @Override
    public synchronized Set<Entry<K, V>> entrySet() {
        return new LinkedHashSet<>(getValuesList());
    }

    @Override
    public boolean equals(Object o) {
        // noinspection SimplifiableIfStatement
        if (o == null || !(o instanceof SortedIndexableMapWrapper)) {
            return false;
        }
        return comparator.equals(((SortedIndexableMapWrapper) o).comparator)
            && baseMap.equals(((SortedIndexableMapWrapper) o).baseMap);
    }

    @Override
    public int hashCode() {
        return baseMap.hashCode();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return baseMap.getOrDefault(key, defaultValue);
    }

    @Override
    public synchronized void forEach(BiConsumer<? super K, ? super V> action) {
        getValuesList().forEach(e -> action.accept(e.getKey(), e.getValue()));
    }

    @Override
    public synchronized void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        clearList();
        baseMap.replaceAll(function);
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        clearList();
        return baseMap.putIfAbsent(key, value);
    }

    @Override
    public synchronized boolean remove(Object key, Object value) {
        clearList();
        return baseMap.remove(key, value);
    }

    @Override
    public synchronized boolean replace(K key, V oldValue, V newValue) {
        clearList();
        return baseMap.replace(key, oldValue, newValue);
    }

    @Override
    public synchronized V replace(K key, V value) {
        clearList();
        return baseMap.replace(key, value);
    }

    @Override
    public synchronized V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        clearList();
        return baseMap.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public synchronized V computeIfPresent(K key,
        BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        clearList();
        return baseMap.computeIfPresent(key, remappingFunction);
    }

    @Override
    public synchronized V compute(K key,
        BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        clearList();
        return baseMap.compute(key, remappingFunction);
    }

    @Override
    public synchronized V merge(K key, V value,
        BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        clearList();
        return baseMap.merge(key, value, remappingFunction);
    }

    @Override
    public synchronized V getByIndex(int index) {
        return getValuesList().get(index).getValue();
    }
}
