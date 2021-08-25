package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableAdapterResults.GetOutput;
import io.deephaven.qst.TableCreator;

import java.util.Objects;

/**
 * Adapts a {@link TableSpec table} into {@link TableCreationLogic}.
 */
final class TableCreationLogicImpl implements TableCreationLogic {
    private final TableSpec table;

    TableCreationLogicImpl(TableSpec table) {
        this.table = Objects.requireNonNull(table);
    }

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        // noinspection RedundantTypeArguments
        return TableCreator.<T, T>create(creation, i -> i, i -> i, table).map().get(table)
                .walk(new GetOutput<>()).out();
    }
}
