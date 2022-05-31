package io.deephaven.engine.util;

import org.jpy.PyDictWrapper;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A collection of methods around retrieving objects from the given Python scope.
 * <p>
 * The scope is likely coming from some sort of Python dictionary. The scope might be local, global, or other.
 *
 * @param <PyObj> the implementation's raw Python object type
 */
public interface PythonScope<PyObj> {
    /**
     * Retrieves a value from the given scope.
     * <p>
     * No conversion is done.
     *
     * @param name the name of the python variable
     * @return the value, or empty
     */
    Optional<PyObj> getValueRaw(String name);

    /**
     * Retrieves all keys from the give scope.
     * <p>
     * No conversion is done.
     * <p>
     * Technically, the keys can be tuples...
     *
     * @return the keys
     */
    Stream<PyObj> getKeysRaw();

    /**
     * Retrieves all keys and values from the given scope.
     * <p>
     * No conversion is done.
     *
     * @return the keys and values
     */
    Stream<Entry<PyObj, PyObj>> getEntriesRaw();

    /**
     * The helper method to turn a raw key into a string key.
     * <p>
     * Note: this assumes that all the keys are strings, which is not always true. Indices can also be tuples. TODO:
     * revise interface as appropriate if this becomes an issue.
     *
     * @param key the raw key
     * @return the string key
     * @throws IllegalArgumentException if the key is not a string
     */
    String convertStringKey(PyObj key);

    /**
     * The helper method to turn a raw value into an implementation specific object.
     * <p>
     * This method should NOT convert PyObj of None type to null - we need to preserve the None object so it works with
     * other Optional return values.
     *
     * @param value the raw value
     * @return the converted object value
     */
    Object convertValue(PyObj value);

    /**
     * Finds out if a variable is in scope
     *
     * @param name the name of the python variable
     * @return true iff the scope contains the variable
     */
    default boolean containsKey(String name) {
        return getValueRaw(name).isPresent();
    }

    /**
     * Equivalent to {@link #getValueRaw(String)}.map({@link #convertValue(PyObj)})
     *
     * @param name the name of the python variable
     * @return the converted object value, or empty
     */
    default Optional<Object> getValue(String name) {
        return getValueRaw(name)
                .map(this::convertValue);
    }

    /**
     * Equivalent to {@link #getValue(String)}.map({@code clazz}.{@link Class#cast(Object)})
     *
     * @param name the name of the python variable
     * @param clazz the class to cast to
     * @param <T> the return type
     * @return the converted casted value, or empty
     */
    default <T> Optional<T> getValue(String name, Class<T> clazz) {
        return getValue(name)
                .map(clazz::cast);
    }

    /**
     * Equivalent to {@link #getValue(String)}.map(x -> (T)x);
     *
     * @param name the name of the python variable
     * @param <T> the return type
     * @return the converted casted value, or empty
     */
    default <T> Optional<T> getValueUnchecked(String name) {
        // noinspection unchecked
        return getValue(name)
                .map(x -> (T) x);
    }

    /**
     * Equivalent to {@link #getKeysRaw()}.map({@link #convertStringKey(PyObj)})
     *
     * @return the string keys
     */
    default Stream<String> getKeys() {
        return getKeysRaw()
                .map(this::convertStringKey);
    }

    /**
     * Equivalent to {@link #getEntriesRaw()}, where the keys have been converted via {@link #convertStringKey(PyObj)}
     * and the values via {@link #convertValue(PyObj)}
     *
     * @return the string keys and converted values
     */
    default Stream<Entry<String, Object>> getEntries() {
        return getEntriesRaw()
                .map(e -> new SimpleImmutableEntry<>(convertStringKey(e.getKey()), convertValue(e.getValue())));
    }

    /**
     * Equivalent to {@link #getKeys()}.collect(someCollector)
     *
     * @return the string keys, as a collection
     */
    default Collection<String> getKeysCollection() {
        return getKeys()
                .collect(Collectors.toList());
    }

    /**
     * Equivalent to {@link #getEntries()}.collect(someMapCollector)
     *
     * @return the string keys and converted values, as a map
     */
    default Map<String, Object> getEntriesMap() {
        return getEntries()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        // we're currently making sure that we don't convert None to null...
        /*
         * // workaround since the collector doesn't work w/ null values //
         * https://bugs.openjdk.java.net/browse/JDK-8148463 return getEntries() .collect( HashMap::new, (map, entry) ->
         * map.put(entry.getKey(), entry.getValue()), HashMap::putAll);
         */
    }

    public PyDictWrapper globals();
}
