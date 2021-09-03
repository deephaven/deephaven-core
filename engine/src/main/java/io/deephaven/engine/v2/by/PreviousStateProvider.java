package io.deephaven.engine.v2.by;

interface PreviousStateProvider<T> {
    T prev();
}
