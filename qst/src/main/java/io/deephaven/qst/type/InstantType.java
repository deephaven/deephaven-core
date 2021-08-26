package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import java.time.Instant;

/**
 * The {@link Instant} type.
 */
@Immutable
@SimpleStyle
public abstract class InstantType extends GenericTypeBase<Instant> {

    public static InstantType instance() {
        return ImmutableInstantType.of();
    }

    @Override
    public final Class<Instant> clazz() {
        return Instant.class;
    }

    @Override
    public final <V extends GenericType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return InstantType.class.getName();
    }
}
