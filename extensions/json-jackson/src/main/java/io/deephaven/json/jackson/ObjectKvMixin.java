//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.json.ObjectKvValue;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Stream;

final class ObjectKvMixin extends Mixin<ObjectKvValue> {

    public ObjectKvMixin(ObjectKvValue options, JsonFactory factory) {
        super(factory, options);
    }

    public Mixin<?> keyMixin() {
        return mixin(options.key());
    }

    public Mixin<?> valueMixin() {
        return mixin(options.value());
    }

    @Override
    public Stream<NativeArrayType<?, ?>> outputTypesImpl() {
        return keyValueOutputTypes().map(Type::arrayType);
    }

    @Override
    public int numColumns() {
        return keyMixin().numColumns() + valueMixin().numColumns();
    }

    @Override
    public Stream<List<String>> paths() {
        final Stream<List<String>> keyPath =
                keyMixin().numColumns() == 1 && keyMixin().paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Key"))
                        : keyMixin().paths();
        final Stream<List<String>> valuePath =
                valueMixin().numColumns() == 1 && valueMixin().paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Value"))
                        : valueMixin().paths();
        return Stream.concat(keyPath, valuePath);
    }

    @Override
    public ValueProcessorKvImpl processor(String context) {
        return innerProcessor();
    }

    Stream<Type<?>> keyValueOutputTypes() {
        return Stream.concat(keyMixin().outputTypesImpl(), valueMixin().outputTypesImpl());
    }

    private ValueProcessorKvImpl innerProcessor() {
        final Mixin<?> key = keyMixin();
        final Mixin<?> value = valueMixin();
        final RepeaterProcessor kp = key.repeaterProcessor(allowMissing(), allowNull());
        final RepeaterProcessor vp = value.repeaterProcessor(allowMissing(), allowNull());
        return new ValueProcessorKvImpl(kp, vp);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new ValueInnerRepeaterProcessor(allowMissing, allowNull, innerProcessor());
    }
}
