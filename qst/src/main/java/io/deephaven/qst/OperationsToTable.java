package io.deephaven.qst;

import io.deephaven.api.TableOperations;

@FunctionalInterface
public interface OperationsToTable<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

    TABLE of(TOPS tableOperations);
}
