/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.updateby.UpdateByClause;
import io.deephaven.api.updateby.UpdateByControl;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;

@Immutable
@NodeStyle
public abstract class UpdateByTable extends ByTableBase {

    public static Builder builder() {
        return ImmutableUpdateByTable.builder();
    }

    public abstract Optional<UpdateByControl> control();

    public abstract List<UpdateByClause> operations();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkNumOperations() {
        if (operations().isEmpty()) {
            throw new IllegalArgumentException("Operations must not be empty");
        }
    }

    public interface Builder extends ByTableBase.Builder<UpdateByTable, Builder> {
        Builder control(UpdateByControl control);

        Builder addOperations(UpdateByClause element);

        Builder addOperations(UpdateByClause... elements);

        Builder addAllOperations(Iterable<? extends UpdateByClause> elements);
    }
}
