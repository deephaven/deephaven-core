package io.deephaven.engine.table.impl.by;

interface PreviousStateProvider<T> {
    T prev();
}
