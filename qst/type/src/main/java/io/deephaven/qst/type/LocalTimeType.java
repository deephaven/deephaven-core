//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.time.LocalTime;

/**
 * The {@link LocalTime} type.
 */
@Immutable
@SingletonStyle
public abstract class LocalTimeType extends GenericTypeBase<LocalTime> {

    public static LocalTimeType of() {
        return ImmutableLocalTimeType.of();
    }

    @Override
    public final Class<LocalTime> clazz() {
        return LocalTime.class;
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return LocalTimeType.class.getName();
    }
}

