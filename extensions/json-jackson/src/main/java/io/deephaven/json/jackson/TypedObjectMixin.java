//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.ObjectField;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.TypedObjectValue;
import io.deephaven.json.jackson.Exceptions.ValueAwareException;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TypedObjectMixin extends Mixin<TypedObjectValue> {

    private final Map<ObjectField, Mixin<?>> sharedFields;
    private final Map<String, ObjectMixin> combinedFields;
    private final int numSharedColumns;
    private final int numSpecificColumns;

    public TypedObjectMixin(TypedObjectValue options, JsonFactory factory) {
        super(factory, options);
        {
            final LinkedHashMap<ObjectField, Mixin<?>> map = new LinkedHashMap<>(options.sharedFields().size());
            for (ObjectField sharedField : options.sharedFields()) {
                map.put(sharedField, mixin(sharedField.options()));
            }
            sharedFields = Collections.unmodifiableMap(map);
        }
        {
            final LinkedHashMap<String, ObjectMixin> map = new LinkedHashMap<>(options.objects().size());
            for (Entry<String, ObjectValue> e : options.objects().entrySet()) {
                map.put(e.getKey(), new ObjectMixin(combinedObject(e.getValue()), factory));
            }
            combinedFields = Collections.unmodifiableMap(map);
        }
        numSharedColumns = sharedFields.values().stream().mapToInt(Mixin::numColumns).sum();
        numSpecificColumns =
                combinedFields.values().stream().mapToInt(ObjectMixin::numColumns).map(x -> x - numSharedColumns).sum();
    }

    @Override
    public int numColumns() {
        return 1 + numSharedColumns + numSpecificColumns;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.concat(
                Stream.of(List.of(options.typeFieldName())),
                Stream.concat(
                        prefixWithKeys(sharedFields),
                        prefixWithKeysAndSkip(combinedFields, numSharedColumns)));
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.concat(
                Stream.of(Type.stringType()),
                Stream.concat(
                        sharedFields.values().stream().flatMap(Mixin::outputTypesImpl),
                        combinedFields.values().stream().map(Mixin::outputTypesImpl)
                                .flatMap(x -> x.skip(numSharedColumns))));
    }

    @Override
    public ValueProcessor processor(String context) {
        final Map<String, Processor> processors = new LinkedHashMap<>(combinedFields.size());
        for (Entry<String, ObjectMixin> e : combinedFields.entrySet()) {
            final String type = e.getKey();
            final ValueProcessor processor = e.getValue().processor(context + "[" + type + "]", true);
            processors.put(type, new Processor(processor));
        }
        return new DiscriminatedProcessor(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        throw new UnsupportedOperationException();
    }

    private ObjectValue combinedObject(ObjectValue objectOpts) {
        final Set<ObjectField> sharedFields = options.sharedFields();
        if (sharedFields.isEmpty()) {
            return objectOpts;
        }
        return ObjectValue.builder()
                .allowUnknownFields(objectOpts.allowUnknownFields())
                .allowMissing(objectOpts.allowMissing())
                .allowedTypes(objectOpts.allowedTypes())
                .addAllFields(sharedFields)
                .addAllFields(objectOpts.fields())
                .build();
    }

    private String parseTypeField(JsonParser parser) throws IOException {
        final String actualFieldName = parser.currentName();
        if (!options.typeFieldName().equals(actualFieldName)) {
            throw new ValueAwareException(String.format("Expected the first field to be '%s', is '%s'",
                    options.typeFieldName(), actualFieldName), parser.currentLocation(), options);
        }
        switch (parser.nextToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parser.getText();
            case VALUE_NULL:
                return null;
            default:
                throw unexpectedToken(parser);
        }
    }

    private static class Processor {
        private final ValueProcessor combinedProcessor;
        private final List<WritableChunk<?>> buffer;
        private List<WritableChunk<?>> specificOut;

        Processor(ValueProcessor combinedProcessor) {
            this.combinedProcessor = Objects.requireNonNull(combinedProcessor);
            this.buffer = new ArrayList<>(combinedProcessor.numColumns());
        }

        void setContext(List<WritableChunk<?>> sharedOut, List<WritableChunk<?>> specifiedOut) {
            this.specificOut = Objects.requireNonNull(specifiedOut);
            buffer.clear();
            buffer.addAll(sharedOut);
            buffer.addAll(specifiedOut);
            combinedProcessor.setContext(buffer);
        }

        void clearContext() {
            combinedProcessor.clearContext();
            buffer.clear();
            specificOut = null;
        }

        ValueProcessor combinedProcessor() {
            return combinedProcessor;
        }

        void notApplicable() {
            // only skip specific fields
            for (WritableChunk<?> wc : specificOut) {
                addNullValue(wc);
            }
        }
    }

    private class DiscriminatedProcessor implements ValueProcessor {

        private final Map<String, Processor> combinedProcessors;

        private WritableObjectChunk<String, ?> typeChunk;
        private List<WritableChunk<?>> sharedChunks;

        public DiscriminatedProcessor(Map<String, Processor> combinedProcessors) {
            this.combinedProcessors = Objects.requireNonNull(combinedProcessors);
        }

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            typeChunk = out.get(0).asWritableObjectChunk();
            sharedChunks = out.subList(1, 1 + numSharedColumns);
            int outIx = 1 + sharedChunks.size();
            for (Processor combinedProcessor : combinedProcessors.values()) {
                final int numColumns = combinedProcessor.combinedProcessor().numColumns();
                final int numSpecificColumns = numColumns - numSharedColumns;
                final List<WritableChunk<?>> specificChunks = out.subList(outIx, outIx + numSpecificColumns);
                combinedProcessor.setContext(sharedChunks, specificChunks);
                outIx += numSpecificColumns;
            }
        }

        @Override
        public void clearContext() {
            typeChunk = null;
            sharedChunks = null;
            for (Processor combinedProcessor : combinedProcessors.values()) {
                combinedProcessor.clearContext();
            }
        }

        @Override
        public int numColumns() {
            return TypedObjectMixin.this.numColumns();
        }

        @Override
        public Stream<Type<?>> columnTypes() {
            return outputTypesImpl();
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (parser.nextToken() == JsonToken.END_OBJECT) {
                        processEmptyObject(parser);
                        return;
                    }
                    if (!parser.hasToken(JsonToken.FIELD_NAME)) {
                        throw new IllegalStateException();
                    }
                    processObjectFields(parser);
                    return;
                case VALUE_NULL:
                    processNullObject(parser);
                    return;
                default:
                    throw unexpectedToken(parser);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            typeChunk.add(options.onMissing().orElse(null));
            for (WritableChunk<?> sharedChunk : sharedChunks) {
                addNullValue(sharedChunk);
            }
            for (Processor processor : combinedProcessors.values()) {
                processor.notApplicable();
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            checkNullAllowed(parser);
            typeChunk.add(options.onNull().orElse(null));
            for (WritableChunk<?> sharedChunk : sharedChunks) {
                addNullValue(sharedChunk);
            }
            for (Processor processor : combinedProcessors.values()) {
                processor.notApplicable();
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            throw new ValueAwareException("Expected a non-empty object", parser.currentLocation(), options);
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            final String typeFieldValue = parseTypeField(parser);
            typeChunk.add(typeFieldValue);
            parser.nextToken();
            boolean foundProcessor = false;
            for (Entry<String, Processor> e : combinedProcessors.entrySet()) {
                final String processorType = e.getKey();
                final Processor processor = e.getValue();
                if (processorType.equals(typeFieldValue)) {
                    processor.combinedProcessor().processCurrentValue(parser);
                    foundProcessor = true;
                } else {
                    processor.notApplicable();
                }
            }
            if (!foundProcessor) {
                if (!options.allowUnknownTypes()) {
                    throw new ValueAwareException(String.format("Unknown type '%s' not allowed", typeFieldValue),
                            parser.currentLocation(), options);
                }
                for (WritableChunk<?> sharedChunk : sharedChunks) {
                    addNullValue(sharedChunk);
                }
                FieldProcessor.skipFields(parser);
            }
        }
    }

    private static void addNullValue(WritableChunk<?> writableChunk) {
        final int size = writableChunk.size();
        writableChunk.fillWithNullValue(size, 1);
        writableChunk.setSize(size + 1);
    }
}
