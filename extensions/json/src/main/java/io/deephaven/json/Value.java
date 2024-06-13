//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The base configuration for JSON values.
 */
public abstract class Value {

    /**
     * The allowed types.
     */
    public abstract Set<JsonValueTypes> allowedTypes();

    /**
     * If the processor should allow a missing JSON value. By default is {@code true}.
     */
    @Default
    public boolean allowMissing() {
        return true;
    }

    /**
     * Wraps the allowed values of {@code this} as {@link SkipValue}. Equivalent to
     * {@code SkipValue.builder().allowMissing(allowMissing()).allowedTypes(allowedTypes()).build()}.
     *
     * @return this allowed values of this as skip options
     */
    public final SkipValue skip() {
        return SkipValue.builder()
                .allowMissing(allowMissing())
                .allowedTypes(allowedTypes())
                .build();
    }

    /**
     * Wraps {@code this} as the value of an {@link ArrayValue}. Equivalent to {@code ArrayOptions.standard(this)}.
     *
     * @return this as the value of an array options
     * @see ArrayValue#standard(Value)
     */
    public final ArrayValue array() {
        return ArrayValue.standard(this);
    }

    /**
     * Wraps {@code this} as a singular field of an {@link ObjectValue}. Equivalent to
     * {@code ObjectOptions.standard(Map.of(name, this))}.
     *
     * @param name the field name
     * @return this as the singular field of an object options
     * @see ObjectValue#standard(Map)
     */
    public final ObjectValue field(String name) {
        return ObjectValue.standard(Map.of(name, this));
    }

    public abstract <T> T walk(Visitor<T> visitor);

    public interface Visitor<T> {

        T visit(StringValue _string);

        T visit(BoolValue _bool);

        T visit(CharValue _char);

        T visit(ByteValue _byte);

        T visit(ShortValue _short);

        T visit(IntValue _int);

        T visit(LongValue _long);

        T visit(FloatValue _float);

        T visit(DoubleValue _double);

        T visit(ObjectValue object);

        T visit(ObjectEntriesValue objectKv);

        T visit(InstantValue instant);

        T visit(InstantNumberValue instantNumber);

        T visit(BigIntegerValue bigInteger);

        T visit(BigDecimalValue bigDecimal);

        T visit(SkipValue skip);

        T visit(TupleValue tuple);

        T visit(TypedObjectValue typedObject);

        T visit(LocalDateValue localDate);

        T visit(ArrayValue array);

        T visit(AnyValue any);
    }

    public interface Builder<V extends Value, B extends Builder<V, B>> {

        B allowMissing(boolean allowMissing);

        B allowedTypes(Set<JsonValueTypes> allowedTypes);

        default B allowedTypes(JsonValueTypes... allowedTypes) {
            return allowedTypes(Collections.unmodifiableSet(EnumSet.copyOf(Arrays.asList(allowedTypes))));
        }

        V build();
    }

    @Check
    final void checkAllowedTypeInvariants() {
        JsonValueTypes.checkAllowedTypeInvariants(allowedTypes());
    }
}
