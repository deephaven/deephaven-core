package io.deephaven.qst.table;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class EmptyTable extends TableBase implements SourceTable {

    public static EmptyTable of(long size) {
        return ImmutableEmptyTable.of(size, TableHeader.empty());
    }

    public static EmptyTable of(long size, TableHeader header) {
        return ImmutableEmptyTable.of(size, header);
    }

    @Parameter
    public abstract long size();

    @Parameter
    public abstract TableHeader header();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
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
