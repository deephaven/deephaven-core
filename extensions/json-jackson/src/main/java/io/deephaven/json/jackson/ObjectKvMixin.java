//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ObjectKvValue;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class ObjectKvMixin extends Mixin<ObjectKvValue> {
    private final Mixin<?> key;
    private final Mixin<?> value;

    public ObjectKvMixin(ObjectKvValue options, JsonFactory factory) {
        super(factory, options);
        key = Mixin.of(options.key(), factory);
        value = Mixin.of(options.value(), factory);
    }

    @Override
    public Stream<NativeArrayType<?, ?>> outputTypesImpl() {
        return Stream.concat(key.outputTypesImpl(), value.outputTypesImpl()).map(Type::arrayType);
    }

    @Override
    public int numColumns() {
        return key.numColumns() + value.numColumns();
    }

    @Override
    public Stream<List<String>> paths() {
        final Stream<List<String>> keyPath =
                key.numColumns() == 1 && key.paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Key"))
                        : key.paths();
        final Stream<List<String>> valuePath =
                value.numColumns() == 1 && value.paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Value"))
                        : value.paths();
        return Stream.concat(keyPath, valuePath);
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ValueProcessorKvImpl();
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new ValueInnerRepeaterProcessor(allowMissing, allowNull, new ValueProcessorKvImpl());
    }

    private class ValueProcessorKvImpl implements ValueProcessor {

        private final RepeaterProcessor keyProcessor;
        private final RepeaterProcessor valueProcessor;

        ValueProcessorKvImpl() {
            this.keyProcessor = key.repeaterProcessor(allowMissing(), allowNull());
            this.valueProcessor = value.repeaterProcessor(allowMissing(), allowNull());
        }

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            final int keySize = keyProcessor.numColumns();
            keyProcessor.setContext(out.subList(0, keySize));
            valueProcessor.setContext(out.subList(keySize, keySize + valueProcessor.numColumns()));
        }

        @Override
        public void clearContext() {
            keyProcessor.clearContext();
            valueProcessor.clearContext();
        }

        @Override
        public int numColumns() {
            return keyProcessor.numColumns() + valueProcessor.numColumns();
        }

        @Override
        public Stream<Type<?>> columnTypes() {
            return Stream.concat(keyProcessor.columnTypes(), valueProcessor.columnTypes());
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    RepeaterProcessor.processObjectKeyValues(parser, keyProcessor, valueProcessor);
                    return;
                case VALUE_NULL:
                    checkNullAllowed(parser);
                    keyProcessor.processNullRepeater(parser);
                    valueProcessor.processNullRepeater(parser);
                    return;
                default:
                    throw unexpectedToken(parser);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            keyProcessor.processMissingRepeater(parser);
            valueProcessor.processMissingRepeater(parser);
        }
    }
}
