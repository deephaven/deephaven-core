package io.deephaven.client.examples;

import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.FilterCondition;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Value;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TableSpec;

import java.util.Collections;

public enum ExampleFunction1 implements TableCreationLogic {
    INSTANCE;

    @Override
    public final <T extends TableOperations<T, T>> T create(TableCreator<T> c) {
        return c.of(TableSpec.empty(100)).view("I=i")
            .where(Collections
                .singletonList(FilterOr.of(FilterCondition.lt(ColumnName.of("I"), Value.of(42L)),
                    FilterCondition.eq(ColumnName.of("I"), Value.of(93L)))));
    }
}
