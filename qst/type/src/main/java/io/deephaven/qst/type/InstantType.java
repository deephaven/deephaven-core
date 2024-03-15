//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.time.Instant;

/**
 * The {@link Instant} type.
 */
@Immutable
@SingletonStyle
public abstract class InstantType extends GenericTypeBase<Instant> {

    public static InstantType of() {
        return ImmutableInstantType.of();
    }

    @Override
    public final Class<Instant> clazz() {
        return Instant.class;
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return InstantType.class.getName();
    }
}
