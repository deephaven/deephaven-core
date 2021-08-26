package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

class TableMapFunctionAdapter {

    static BiFunction<Object, Table, Table> of(Function<Table, Table> f) {
        return new Simple(f);
    }

    static class Simple implements BiFunction<Object, Table, Table>, Serializable {

        private static final long serialVersionUID = 1L;

        private final Function<Table, Table> function;

        private Simple(Function<Table, Table> function) {
            this.function = Objects.requireNonNull(function);
        }

        @Override
        public final Table apply(Object key, Table table) {
            return function.apply(table);
        }

        @Override
        public final int hashCode() {
            // flip every other bit, 5 = 0101
            return function.hashCode() ^ 0x55555555;
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Simple other = (Simple) o;
            return function.equals(other.function);
        }
    }
}
