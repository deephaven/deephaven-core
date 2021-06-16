package io.deephaven.qst.table;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class EmptyTable extends TableBase implements SourceTable {

    public static EmptyTable of(long size) {
        return ImmutableEmptyTable.of(TableHeader.empty(), size);
    }

    public static EmptyTable of(TableHeader header, long size) {
        return ImmutableEmptyTable.of(header, size);
    }

    @Parameter
    public abstract TableHeader header();

    @Parameter
    public abstract long size();

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
    final void checkSize() {
        if (size() < 0) {
            throw new IllegalArgumentException("Must have non-negative size");
        }
    }
}
