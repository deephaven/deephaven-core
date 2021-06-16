package io.deephaven.qst.table;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class QueryScopeTable extends TableBase implements SourceTable {

    public static QueryScopeTable of(TableHeader header, String variableName) {
        return ImmutableQueryScopeTable.of(header, variableName);
    }

    @Parameter
    public abstract TableHeader header();

    @Parameter
    public abstract String variableName();

    @Override
    public final <V extends Table.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends SourceTable.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkVariableName() {
        // todo
    }
}
