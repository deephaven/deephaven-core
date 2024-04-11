//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

/**
 * The base configuration for JSON values.
 */
public abstract class ValueOptions {

    /**
     * The allowed types.
     */
    public abstract EnumSet<JsonValueTypes> allowedTypes();

    /**
     * If the processor should allow a missing JSON value. By default is {@code true}.
     */
    @Default
    public boolean allowMissing() {
        return true;
    }

    /**
     * Wraps the allowed values of {@code this} as {@link SkipOptions}. Equivalent to
     * {@code SkipOptions.builder().allowMissing(allowMissing()).allowedTypes(allowedTypes()).build()}.
     *
     * @return this allowed values of this as skip options
     */
    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowMissing(allowMissing())
                .allowedTypes(allowedTypes())
                .build();
    }

    /**
     * Wraps {@code this} as the value of an {@link ArrayOptions}. Equivalent to {@code ArrayOptions.standard(this)}.
     *
     * @return this as the value of an array options
     * @see ArrayOptions#standard(ValueOptions)
     */
    public final ArrayOptions array() {
        return ArrayOptions.standard(this);
    }

    /**
     * Wraps {@code this} as a singular field of an {@link ObjectOptions}. Equivalent to
     * {@code ObjectOptions.standard(Map.of(name, this))}.
     *
     * @param name the field name
     * @return this as the singular field of an object options
     * @see ObjectOptions#standard(Map)
     */
    public final ObjectOptions field(String name) {
        return ObjectOptions.standard(Map.of(name, this));
    }

    public abstract <T> T walk(Visitor<T> visitor);

    public interface Visitor<T> {

        T visit(StringOptions _string);

        T visit(BoolOptions _bool);

        T visit(CharOptions _char);

        T visit(ByteOptions _byte);

        T visit(ShortOptions _short);

        T visit(IntOptions _int);

        T visit(LongOptions _long);

        T visit(FloatOptions _float);

        T visit(DoubleOptions _double);

        T visit(ObjectOptions object);

        T visit(ObjectKvOptions objectKv);

        T visit(InstantOptions instant);

        T visit(InstantNumberOptions instantNumber);

        T visit(BigIntegerOptions bigInteger);

        T visit(BigDecimalOptions bigDecimal);

        T visit(SkipOptions skip);

        T visit(TupleOptions tuple);

        T visit(TypedObjectOptions typedObject);

        T visit(LocalDateOptions localDate);

        T visit(ArrayOptions array);

        T visit(AnyOptions any);
    }

    public interface Builder<V extends ValueOptions, B extends Builder<V, B>> {

        B allowMissing(boolean allowMissing);

        B allowedTypes(EnumSet<JsonValueTypes> allowedTypes);

        default B allowedTypes(JsonValueTypes... allowedTypes) {
            final EnumSet<JsonValueTypes> set = EnumSet.noneOf(JsonValueTypes.class);
            set.addAll(Arrays.asList(allowedTypes));
            return allowedTypes(set);
        }

        V build();
    }

    @Check
    final void checkAllowedTypeInvariants() {
        JsonValueTypes.checkAllowedTypeInvariants(allowedTypes());
    }
}
