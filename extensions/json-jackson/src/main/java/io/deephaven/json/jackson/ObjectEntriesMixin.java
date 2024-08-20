//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ObjectEntriesValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class ObjectEntriesMixin extends Mixin<ObjectEntriesValue> {
    private final Mixin<?> key;
    private final Mixin<?> value;

    public ObjectEntriesMixin(ObjectEntriesValue options, JsonFactory factory) {
        super(factory, options);
        key = Mixin.of(options.key(), factory);
        value = Mixin.of(options.value(), factory);
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.concat(key.outputTypesImpl(), value.outputTypesImpl()).map(Type::arrayType);
    }

    @Override
    public int outputSize() {
        return key.outputSize() + value.outputSize();
    }

    @Override
    public Stream<List<String>> paths() {
        final Stream<List<String>> keyPath =
                key.outputSize() == 1 && key.paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Key"))
                        : key.paths();
        final Stream<List<String>> valuePath =
                value.outputSize() == 1 && value.paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Value"))
                        : value.paths();
        return Stream.concat(keyPath, valuePath);
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ObjectEntriesMixinProcessor();
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new ValueInnerRepeaterProcessor(new ObjectEntriesMixinProcessor());
    }

    private class ObjectEntriesMixinProcessor extends ValueProcessorMixinBase {

        private final RepeaterProcessor keyProcessor;
        private final RepeaterProcessor valueProcessor;

        ObjectEntriesMixinProcessor() {
            this.keyProcessor = key.repeaterProcessor();
            this.valueProcessor = value.repeaterProcessor();
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
        protected void processCurrentValueImpl(JsonParser parser) throws IOException {
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
        protected void processMissingImpl(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            keyProcessor.processMissingRepeater(parser);
            valueProcessor.processMissingRepeater(parser);
        }
    }
}
