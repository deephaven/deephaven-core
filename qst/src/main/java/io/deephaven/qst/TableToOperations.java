package io.deephaven.qst;

import io.deephaven.api.TableOperations;

@FunctionalInterface
public interface TableToOperations<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

    TOPS of(TABLE table);
}
