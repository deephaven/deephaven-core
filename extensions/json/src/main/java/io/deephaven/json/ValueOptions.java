//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The base configuration for JSON values.
 *
 * @see StringOptions
 */
public abstract class ValueOptions implements ObjectProcessor.Provider, NamedObjectProcessor.Provider {

    public abstract Set<JsonValueTypes> desiredTypes();

    /**
     * If the processor should allow a missing JSON value. By default, is {@code true}.
     */
    @Default
    public boolean allowMissing() {
        return true;
    }

    /**
     * Creates a default object processor of type {@code inputType} from {@code this}. Callers wanting more control are
     * encouraged to depend on a specific implementation and construct an object processor from {@code this} more
     * explicitly.
     *
     * @param inputType the input type
     * @return the object processor
     * @param <T> the input type
     */
    @Override
    public final <T> ObjectProcessor<? super T> processor(Class<T> inputType) {
        return JsonProcessorProvider.serviceLoader().provider(this).processor(inputType);
    }

    /**
     * Creates a default named object processor of type {@code inputType} from {@code this}. Callers wanting more
     * control are encouraged to depend on a specific implementation and construct a named object processor from
     * {@code this} more explicitly.
     *
     * @param inputType the input type
     * @return the named object processor
     * @param <T> the input type
     */
    @Override
    public final <T> NamedObjectProcessor<? super T> named(Class<T> inputType) {
        return JsonProcessorProvider.serviceLoader().namedProvider(this).named(inputType);
    }

    public final SkipOptions skip() {
        return SkipOptions.builder()
                .allowMissing(allowMissing())
                .desiredTypes(desiredTypes())
                .build();
    }

    public final ArrayOptions array() {
        return ArrayOptions.standard(this);
    }

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

        // todo: bool, short, byte, char
    }

    public interface Builder<V extends ValueOptions, B extends Builder<V, B>> {

        B allowMissing(boolean allowMissing);

        B desiredTypes(Set<JsonValueTypes> desiredTypes);

        default B desiredTypes(JsonValueTypes... desiredTypes) {
            return desiredTypes(Set.of(desiredTypes));
        }

        V build();
    }

    abstract EnumSet<JsonValueTypes> allowableTypes();

    @Check
    void checkIllegalTypes() {
        for (JsonValueTypes type : desiredTypes()) {
            if (!allowableTypes().contains(type)) {
                throw new IllegalArgumentException("Unexpected type " + type);
            }
        }
    }

    @Check
    final void checkInvariants() {
        JsonValueTypes.checkInvariants(desiredTypes());
    }

    public final boolean allowNull() {
        return desiredTypes().contains(JsonValueTypes.NULL);
    }

    public final boolean allowString() {
        return desiredTypes().contains(JsonValueTypes.STRING);
    }

    public final boolean allowNumberInt() {
        return desiredTypes().contains(JsonValueTypes.INT);
    }

    public final boolean allowDecimal() {
        return desiredTypes().contains(JsonValueTypes.DECIMAL);
    }

    public final boolean allowBool() {
        return desiredTypes().contains(JsonValueTypes.BOOL);
    }

    public final boolean allowObject() {
        return desiredTypes().contains(JsonValueTypes.OBJECT);
    }

    public final boolean allowArray() {
        return desiredTypes().contains(JsonValueTypes.ARRAY);
    }

    // for nested / typedescr cases
    ValueOptions withMissingSupport() {
        if (allowMissing()) {
            return this;
        }
        throw new UnsupportedOperationException(); // todo
    }
}
