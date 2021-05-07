package io.deephaven.db.v2.by;

interface PreviousStateProvider<T> {
    T prev();
}
