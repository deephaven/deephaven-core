//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see io.deephaven.api.TableOperations#naturalJoin(Object, Collection, Collection)
 */
@Immutable
@NodeStyle
public abstract class NaturalJoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableNaturalJoinTable.builder();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends Join.Builder<NaturalJoinTable, Builder> {

    }
}
