package io.deephaven.qst.table;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class AsOfJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableAsOfJoinTable.builder();
    }

    @Value.Default
    public AsOfJoinRule rule() {
        return AsOfJoinRule.LESS_THAN_EQUAL;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<AsOfJoinTable, Builder> {

        Builder rule(AsOfJoinRule rule);
    }
}
