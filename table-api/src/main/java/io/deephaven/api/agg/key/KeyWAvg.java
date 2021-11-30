package io.deephaven.api.agg.key;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class KeyWAvg extends KeyBase {

    public static KeyWAvg of(ColumnName weight) {
        return ImmutableKeyWAvg.of(weight);
    }

    @Parameter
    public abstract ColumnName weight();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
