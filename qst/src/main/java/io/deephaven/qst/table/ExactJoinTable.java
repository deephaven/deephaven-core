package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see io.deephaven.api.TableOperations#exactJoin(Object, Collection, Collection)
 */
@Immutable
@NodeStyle
public abstract class ExactJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableExactJoinTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<ExactJoinTable, Builder> {

    }
}
