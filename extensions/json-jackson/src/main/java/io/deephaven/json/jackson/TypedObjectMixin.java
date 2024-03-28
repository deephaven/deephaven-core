//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.ObjectFieldOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.TypedObjectOptions;
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

final class TypedObjectMixin extends Mixin<TypedObjectOptions> {
    public TypedObjectMixin(TypedObjectOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
        return 1
                + options.sharedFields()
                        .stream()
                        .map(ObjectFieldOptions::options)
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
                        prefixWithKeys(options.objects().values().stream().map(ObjectOptions::fields)
                                .flatMap(Collection::stream).collect(Collectors.toList()))));
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.concat(
                Stream.of(Type.stringType()),
                Stream.concat(
                        options.sharedFields().stream().map(ObjectFieldOptions::options).map(this::mixin)
                                .flatMap(Mixin::outputTypes),
                        options.objects().values().stream().map(this::mixin).flatMap(Mixin::outputTypes)));
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        final WritableObjectChunk<String, ?> typeOut = out.get(0).asWritableObjectChunk();
        final List<WritableChunk<?>> sharedFields = out.subList(1, 1 + options.sharedFields().size());
        final Map<String, Processor> processors = new LinkedHashMap<>(options.objects().size());
        int outIx = 1 + sharedFields.size();
        for (Entry<String, ObjectOptions> e : options.objects().entrySet()) {
            final String type = e.getKey();
            final ObjectOptions specificOpts = e.getValue();
            final int numSpecificFields = mixin(specificOpts).numColumns();
            final List<WritableChunk<?>> specificChunks = out.subList(outIx, outIx + numSpecificFields);
            final List<WritableChunk<?>> allChunks = concat(sharedFields, specificChunks);
            final ObjectOptions combinedObject = combinedObject(specificOpts);
            final ValueProcessor processor = mixin(combinedObject).processor(context + "[" + type + "]", allChunks);
            processors.put(type, new Processor(processor, specificChunks));
            outIx += numSpecificFields;
        }
        if (outIx != out.size()) {
            throw new IllegalStateException();
        }
        return new DiscriminatedProcessor(typeOut, processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
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

    private ObjectOptions combinedObject(ObjectOptions objectOpts) {
        final Set<ObjectFieldOptions> sharedFields = options.sharedFields();
        if (sharedFields.isEmpty()) {
            return objectOpts;
        }
        return ObjectOptions.builder()
                .allowUnknownFields(objectOpts.allowUnknownFields())
                .allowMissing(objectOpts.allowMissing())
                .desiredTypes(objectOpts.desiredTypes())
                .addAllFields(sharedFields)
                .addAllFields(objectOpts.fields())
                .build();
    }

    private String parseTypeField(JsonParser parser) throws IOException {
        final String currentFieldName = parser.currentName();
        if (!options.typeFieldName().equals(currentFieldName)) {
            throw new IOException("Can only processes when first field in object is the type");
        }
        switch (parser.nextToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parser.getText();
            case VALUE_NULL:
                if (!options.allowNull()) {
                    throw Parsing.mismatch(parser, String.class);
                }
                return null;
            default:
                throw Parsing.mismatch(parser, String.class);
        }
    }

    private static class Processor {
        private final ValueProcessor valueProcessor;
        private final List<WritableChunk<?>> specificChunks;

        public Processor(ValueProcessor valueProcessor, List<WritableChunk<?>> specificChunks) {
            this.valueProcessor = Objects.requireNonNull(valueProcessor);
            this.specificChunks = Objects.requireNonNull(specificChunks);
        }

        ValueProcessor processor() {
            return valueProcessor;
        }

        void notApplicable() {
            for (WritableChunk<?> wc : specificChunks) {
                final int size = wc.size();
                wc.fillWithNullValue(size, 1);
                wc.setSize(size + 1);
            }
        }
    }

    private class DiscriminatedProcessor implements ValueProcessor {

        private final WritableObjectChunk<String, ?> typeOut;
        private final Map<String, Processor> processors;

        public DiscriminatedProcessor(WritableObjectChunk<String, ?> typeOut, Map<String, Processor> processors) {
            this.typeOut = Objects.requireNonNull(typeOut);
            this.processors = Objects.requireNonNull(processors);
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
                    throw Parsing.mismatch(parser, Object.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            // onMissingType()?
            typeOut.add(null);
            for (Processor processor : processors.values()) {
                processor.notApplicable();
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            if (!options.allowNull()) {
                throw Parsing.mismatch(parser, Object.class);
            }
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
