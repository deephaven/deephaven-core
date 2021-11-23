package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.locations.ImmutableTableKey;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.engine.table.impl.locations.TableKey;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.Function;


/**
 * A registry for states keyed by {@link TableKey}.
 */
public class TableKeyStateRegistry<VALUE_TYPE> {

    private final KeyedObjectHashMap<TableKey, State<VALUE_TYPE>> registeredTableMaps =
            new KeyedObjectHashMap<>(StateKey.getInstance());

    /**
     * Get (or create if none exists) a value for the supplied {@link TableKey}.
     *
     * @param tableKey The table key
     * @return The associated value
     */
    public VALUE_TYPE computeIfAbsent(@NotNull final TableKey tableKey,
            @NotNull final Function<TableKey, VALUE_TYPE> valueFactory) {
        return registeredTableMaps.putIfAbsent(tableKey, State::new, valueFactory).value;
    }

    public void forEach(@NotNull final Consumer<VALUE_TYPE> consumer) {
        registeredTableMaps.values().forEach(s -> consumer.accept(s.value));
    }

    public void clear() {
        registeredTableMaps.clear();
    }

    private static class State<VALUE_TYPE> {

        private final ImmutableTableKey key;

        private final VALUE_TYPE value;

        private State(@NotNull final TableKey key, @NotNull final Function<TableKey, VALUE_TYPE> valueFactory) {
            this.key = key.makeImmutable();
            value = valueFactory.apply(key);
        }
    }

    private static class StateKey<VALUE_TYPE> extends KeyedObjectKey.Basic<TableKey, State<VALUE_TYPE>> {

        @SuppressWarnings("rawtypes")
        private static final StateKey INSTANCE = new StateKey();

        private static <VALUE_TYPE> KeyedObjectKey<TableKey, State<VALUE_TYPE>> getInstance() {
            // noinspection unchecked
            return (KeyedObjectKey<TableKey, State<VALUE_TYPE>>) INSTANCE;
        }

        private StateKey() {}

        @Override
        public TableKey getKey(@NotNull final State<VALUE_TYPE> state) {
            return state.key;
        }
    }
}
