package io.deephaven.qst.table;

import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class NaturalJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableNaturalJoinTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<NaturalJoinTable, Builder> {

    }
}
