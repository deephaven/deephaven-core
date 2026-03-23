//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.time.Period;

/**
 * The {@link Period} type.
 */
@Immutable
@SingletonStyle
public abstract class PeriodType extends GenericTypeBase<Period> {

    public static PeriodType of() {
        return ImmutablePeriodType.of();
    }

    @Override
    public final Class<Period> clazz() {
        return Period.class;
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return PeriodType.class.getName();
    }
}
