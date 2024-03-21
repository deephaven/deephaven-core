//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.OptionalInt;

/**
 * @see TableOperations#join(Object, Collection, Collection)
 * @see TableOperations#join(Object, Collection, Collection, int)
 */
@Immutable
@NodeStyle
public abstract class JoinTable extends JoinBase {

    public static Builder builder() {
        return ImmutableJoinTable.builder();
    }

    public abstract OptionalInt reserveBits();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends Join.Builder<JoinTable, Builder> {

        Builder reserveBits(int reserveBits);
    }
}
