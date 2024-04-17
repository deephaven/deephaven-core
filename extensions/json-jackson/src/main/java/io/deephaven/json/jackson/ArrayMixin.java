//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.json.ArrayValue;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ArrayMixin extends Mixin<ArrayValue> {

    public ArrayMixin(ArrayValue options, JsonFactory factory) {
        super(factory, options);
    }

    Mixin<?> element() {
        return mixin(options.element());
    }

    @Override
    public int numColumns() {
        return element().numColumns();
    }

    @Override
    public Stream<List<String>> paths() {
        return element().paths();
    }

    @Override
    public Stream<NativeArrayType<?, ?>> outputTypesImpl() {
        return elementOutputTypes().map(Type::arrayType);
    }

    @Override
    public ValueProcessor processor(String context) {
        return innerProcessor();
    }

    Stream<? extends Type<?>> elementOutputTypes() {
        return element().outputTypesImpl();
    }

    RepeaterProcessor elementRepeater() {
        return element().repeaterProcessor(allowMissing(), allowNull());
    }

    private ArrayValueProcessor innerProcessor() {
        return new ArrayValueProcessor(elementRepeater());
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        // For example:
        // double (element())
        // double[] (processor())
        // double[][] (repeater())
        // return new ArrayOfArrayRepeaterProcessor(allowMissing, allowNull);
        return new ValueInnerRepeaterProcessor(allowMissing, allowNull, innerProcessor(),
                elementOutputTypes().collect(Collectors.toList()));
    }
}
