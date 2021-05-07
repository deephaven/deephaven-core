package io.deephaven.db.v2;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLookupKey;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;


/**
 * A registry for states keyed by {@link TableKey}.
 */
public class TableKeyStateRegistry<VALUE_TYPE> {

    private final KeyedObjectHashMap<TableKey, State<VALUE_TYPE>> registeredTableMaps = new KeyedObjectHashMap<>(StateKey.getInstance());

    /**
     * Get (or create if none exists) a value for the supplied {@link TableKey}.
     *
     * @param tableKey The table key
     * @return The associated value
     */
    public VALUE_TYPE computeIfAbsent(@NotNull final TableKey tableKey, @NotNull final Function<TableKey, VALUE_TYPE> valueFactory) {
        return registeredTableMaps.putIfAbsent(tableKey, State::new, valueFactory).value;
    }

    public void forEach(@NotNull final Consumer<VALUE_TYPE> consumer) {
        registeredTableMaps.values().forEach(s -> consumer.accept(s.value));
    }

    public void clear() {
        registeredTableMaps.clear();
    }

    private static class State<VALUE_TYPE> {

        private final TableKey key;

        private final VALUE_TYPE value;

        private State(@NotNull final TableKey key, @NotNull final Function<TableKey, VALUE_TYPE> valueFactory) {
            this.key = TableLookupKey.getImmutableKey(key);
            value = valueFactory.apply(key);
        }
    }

    private static class StateKey<VALUE_TYPE> extends TableKey.KeyedObjectKeyImpl<State<VALUE_TYPE>> {

        private static final StateKey INSTANCE = new StateKey();

        private static <VALUE_TYPE> KeyedObjectKey<TableKey, State<VALUE_TYPE>> getInstance() {
            //noinspection unchecked
            return (KeyedObjectKey<TableKey, State<VALUE_TYPE>>) INSTANCE;
        }

        private StateKey() {
        }

        @Override
        public TableKey getKey(@NotNull final State<VALUE_TYPE> state) {
            return state.key;
        }
    }
}
