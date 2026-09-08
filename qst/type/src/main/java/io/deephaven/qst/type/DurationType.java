//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.time.Duration;

/**
 * The {@link Duration} type.
 */
@Immutable
@SingletonStyle
public abstract class DurationType extends GenericTypeBase<Duration> {

    public static DurationType of() {
        return ImmutableDurationType.of();
    }

    @Override
    public final Class<Duration> clazz() {
        return Duration.class;
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return DurationType.class.getName();
    }
}
