//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst;

import io.deephaven.api.TableOperations;

/**
 * An encapsulation of the logic to create a single table, when the table type is also {@link TableOperations}.
 */
@FunctionalInterface
public interface TableCreationLogic1Input {

    <T extends TableOperations<T, T>> T create(T t1);
}
