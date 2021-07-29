package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see TableOperations#leftJoin(Object, Collection, Collection)
 */
@Immutable
@NodeStyle
public abstract class LeftJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableLeftJoinTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<LeftJoinTable, Builder> {

    }
}
