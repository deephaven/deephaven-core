package io.deephaven.qst.table;

import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see io.deephaven.api.TableOperations#raj(Object, Collection, Collection, ReverseAsOfJoinRule)
 */
@Immutable
@NodeStyle
public abstract class ReverseAsOfJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableReverseAsOfJoinTable.builder();
    }

    @Value.Default
    public ReverseAsOfJoinRule rule() {
        return ReverseAsOfJoinRule.GREATER_THAN_EQUAL;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<ReverseAsOfJoinTable, Builder> {

        Builder rule(ReverseAsOfJoinRule rule);
    }
}
