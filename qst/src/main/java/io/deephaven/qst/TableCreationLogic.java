package io.deephaven.qst;

import io.deephaven.api.TableOperations;

/**
 * An encapsulation of the logic to create a single table, when the table type is also {@link TableOperations}.
 */
@FunctionalInterface
public interface TableCreationLogic {

    <T extends TableOperations<T, T>> T create(TableCreator<T> creation);
}
