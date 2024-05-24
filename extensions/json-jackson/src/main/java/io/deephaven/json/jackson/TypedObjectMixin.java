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
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TypedObjectMixin extends Mixin<TypedObjectValue> {
    public TypedObjectMixin(TypedObjectValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
        return 1
                + options.sharedFields()
                        .stream()
                        .map(ObjectField::options)
                        .map(this::mixin)
                        .mapToInt(Mixin::numColumns)
                        .sum()
                + options.objects()
                        .values()
                        .stream()
                        .map(this::mixin)
                        .mapToInt(Mixin::numColumns)
                        .sum();
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.concat(
                Stream.of(List.of(options.typeFieldName())),
                Stream.concat(
                        prefixWithKeys(options.sharedFields()),
                        prefixWithKeys(options.objects().values().stream().map(ObjectValue::fields)
                                .flatMap(Collection::stream).collect(Collectors.toList()))));
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.concat(
                Stream.of(Type.stringType()),
                Stream.concat(
                        options.sharedFields().stream().map(ObjectField::options).map(this::mixin)
                                .flatMap(Mixin::outputTypesImpl),
                        options.objects().values().stream().map(this::mixin).flatMap(Mixin::outputTypesImpl)));
    }

    @Override
    public ValueProcessor processor(String context) {
        final Map<String, Processor> processors = new LinkedHashMap<>(options.objects().size());
        for (Entry<String, ObjectValue> e : options.objects().entrySet()) {
            final String type = e.getKey();
            final ObjectValue specificOpts = e.getValue();
            final ObjectValue combinedObject = combinedObject(specificOpts);
            final ValueProcessor processor = mixin(combinedObject).processor(context + "[" + type + "]");
            processors.put(type, new Processor(processor));
        }
        return new DiscriminatedProcessor(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        throw new UnsupportedOperationException();
    }

    private static <T> List<T> concat(List<T> x, List<T> y) {
        if (x.isEmpty()) {
            return y;
        }
        if (y.isEmpty()) {
            return x;
        }
        final List<T> out = new ArrayList<>(x.size() + y.size());
        out.addAll(x);
        out.addAll(y);
        return out;
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
        final String currentFieldName = parser.currentName();
        if (!options.typeFieldName().equals(currentFieldName)) {
            throw new IOException("Can only process when first field in object is the type");
        }
        switch (parser.nextToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parser.getText();
            case VALUE_NULL:
                checkNullAllowed(parser);
                return null;
            default:
                throw Exceptions.notAllowed(parser, this);
        }
    }

    private static class Processor {
        private final ValueProcessor valueProcessor;
        private List<WritableChunk<?>> specificOut;

        public Processor(ValueProcessor valueProcessor) {
            this.valueProcessor = Objects.requireNonNull(valueProcessor);
        }

        void setContext(List<WritableChunk<?>> sharedOut, List<WritableChunk<?>> specifiedOut) {
            this.specificOut = Objects.requireNonNull(specifiedOut);
            valueProcessor.setContext(concat(sharedOut, specifiedOut));
        }

        void clearContext() {
            valueProcessor.clearContext();
            specificOut = null;
        }

        ValueProcessor processor() {
            return valueProcessor;
        }

        void notApplicable() {
            // only skip specific fields
            for (WritableChunk<?> wc : specificOut) {
                final int size = wc.size();
                wc.fillWithNullValue(size, 1);
                wc.setSize(size + 1);
            }
        }
    }

    private class DiscriminatedProcessor implements ValueProcessor {

        private WritableObjectChunk<String, ?> typeOut;
        private List<WritableChunk<?>> sharedFields;
        private final Map<String, Processor> processors;
        private final int numColumns;

        public DiscriminatedProcessor(Map<String, Processor> processors) {
            this.processors = Objects.requireNonNull(processors);
            this.numColumns = TypedObjectMixin.this.numColumns();
        }

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            typeOut = out.get(0).asWritableObjectChunk();
            sharedFields = out.subList(1, 1 + options.sharedFields().size());
            int outIx = 1 + sharedFields.size();
            for (Processor value : processors.values()) {
                final int numColumns = value.processor().numColumns();
                final int numSpecificFields = numColumns - options.sharedFields().size();
                final List<WritableChunk<?>> specificChunks = out.subList(outIx, outIx + numSpecificFields);
                value.setContext(sharedFields, specificChunks);
                outIx += numSpecificFields;
            }
        }

        @Override
        public void clearContext() {
            typeOut = null;
            sharedFields = null;
            for (Processor value : processors.values()) {
                value.clearContext();
            }
        }

        @Override
        public int numColumns() {
            return numColumns;
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
                    // fall-through
                case FIELD_NAME:
                    processObjectFields(parser);
                    break;
                case VALUE_NULL:
                    processNullObject(parser);
                    break;
                default:
                    throw unexpectedToken(parser);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            // onMissingType()?
            typeOut.add(null);
            for (Processor processor : processors.values()) {
                processor.notApplicable();
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            checkNullAllowed(parser);
            // onNullType()?
            typeOut.add(null);
            for (Processor processor : processors.values()) {
                processor.notApplicable();
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // this logic should be equivalent to processObjectFields w/ no fields
            // suggests that maybe this branch should be an error b/c we _need_ type field?
            throw new IOException("no field");
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            final String typeFieldValue = parseTypeField(parser);
            if (!options.allowUnknownTypes()) {
                if (!processors.containsKey(typeFieldValue)) {
                    throw new IOException(String.format("Unmapped type '%s'", typeFieldValue));
                }
            }
            typeOut.add(typeFieldValue);
            if (parser.nextToken() == JsonToken.END_OBJECT) {
                for (Processor processor : processors.values()) {
                    processor.notApplicable();
                }
                return;
            }
            if (!parser.hasToken(JsonToken.FIELD_NAME)) {
                throw new IllegalStateException();
            }
            for (Entry<String, Processor> e : processors.entrySet()) {
                final String processorType = e.getKey();
                final Processor processor = e.getValue();
                if (processorType.equals(typeFieldValue)) {
                    processor.processor().processCurrentValue(parser);
                } else {
                    processor.notApplicable();
                }
            }
        }
    }
}
