/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst;

import io.deephaven.api.TableOperations;

import java.util.Objects;

class TableCreationLogicAndThen implements TableCreationLogic {
    private final TableCreationLogic in;
    private final TableCreationLogic1Input transform;

    TableCreationLogicAndThen(TableCreationLogic in, TableCreationLogic1Input transform) {
        this.in = Objects.requireNonNull(in);
        this.transform = Objects.requireNonNull(transform);
    }

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        return transform.create(in.create(creation));
    }
}
