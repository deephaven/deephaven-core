package io.deephaven.qst;

import io.deephaven.api.TableOperations;

/**
 * An encapsulation of the logic to return a labeled tables, when the table type is also {@link TableOperations}.
 */
@FunctionalInterface
public interface TableCreationLabeledLogic {

    <T extends TableOperations<T, T>> LabeledValues<T> create(TableCreator<T> creation);
}
