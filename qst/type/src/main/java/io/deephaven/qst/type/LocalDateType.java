//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.time.LocalDate;

/**
 * The {@link LocalDate} type.
 */
@Immutable
@SingletonStyle
public abstract class LocalDateType extends GenericTypeBase<LocalDate> {

    public static LocalDateType of() {
        return ImmutableLocalDateType.of();
    }

    @Override
    public final Class<LocalDate> clazz() {
        return LocalDate.class;
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return LocalDateType.class.getName();
    }
}

