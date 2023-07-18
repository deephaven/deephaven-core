/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import io.deephaven.qst.type.BoxedType.Visitor;
import org.immutables.value.Value.Immutable;

@Immutable
@SingletonStyle
public abstract class BoxedBooleanType extends BoxedTypeBase<Boolean> {

    public static BoxedBooleanType of() {
        return ImmutableBoxedBooleanType.of();
    }

    @Override
    public final Class<Boolean> clazz() {
        return Boolean.class;
    }

    @Override
    public final BooleanType primitiveType() {
        return BooleanType.instance();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
