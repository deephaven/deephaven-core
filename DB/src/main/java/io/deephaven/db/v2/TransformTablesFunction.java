package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import java.util.Objects;
import java.util.function.BiFunction;

class TransformTablesFunction {

    private final TableDefinition returnDefinition;
    private final BiFunction<Object, Table, Table> function;
    // We need this boolean to distinguish what null means for returnDefinition.
    // The user may want to explicitly call transformTablesWithKey(null, function)
    private final boolean isExplicit;

    public TransformTablesFunction(BiFunction<Object, Table, Table> function) {
        this.returnDefinition = null;
        this.function = function;
        this.isExplicit = false;
    }

    public TransformTablesFunction(TableDefinition returnDefinition, BiFunction<Object, Table, Table> function) {
        this.returnDefinition = returnDefinition;
        this.function = Objects.requireNonNull(function);
        this.isExplicit = true;
    }

    public TableMap apply(TableMap tableMap) {
        if (isExplicit) {
            return tableMap.transformTablesWithKey(returnDefinition, function);
        } else {
            return tableMap.transformTablesWithKey(function);
        }
    }

    public Table apply(Object key, Table table) {
        return function.apply(key, table);
    }
}
