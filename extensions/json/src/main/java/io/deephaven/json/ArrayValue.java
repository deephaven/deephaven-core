//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * A "typed array", where all the elements in the {@link JsonValueTypes#ARRAY} have the same {@link #element()} type.
 *
 * <p>
 * For example, the JSON value {@code [1, 42, 43, 13]} might be modelled as
 * {@code ArrayOptions.standard(IntOptions.standard())}.
 */
@Immutable
@BuildableStyle
public abstract class ArrayValue extends ValueRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableArrayValue.builder();
    }

    /**
     * The standard array options. Allows missing and accepts {@link JsonValueTypes#arrayOrNull()}.
     *
     * @param element the element type
     * @return the standard array options
     */
    public static ArrayValue standard(Value element) {
        return builder().element(element).build();
    }

    /**
     * The strict array options. Disallows missing and accepts {@link JsonValueTypes#array()}.
     *
     * @param element the element type
     * @return the strict array options
     */
    public static ArrayValue strict(Value element) {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.array())
                .element(element)
                .build();
    }

    /**
     * The type for the elements of the array.
     */
    public abstract Value element();

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#arrayOrNull()}. By default is
     * {@link JsonValueTypes#arrayOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.arrayOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.arrayOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends Value.Builder<ArrayValue, Builder> {

        Builder element(Value options);
    }
}
