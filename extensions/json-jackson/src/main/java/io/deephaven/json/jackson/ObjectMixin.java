//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.ObjectField;
import io.deephaven.json.ObjectField.RepeatedBehavior;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.jackson.RepeaterProcessor.Context;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

final class ObjectMixin extends Mixin<ObjectValue> {

    public ObjectMixin(ObjectValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return options.fields()
                .stream()
                .map(ObjectField::options)
                .map(this::mixin)
                .flatMap(Mixin::outputTypesImpl);
    }

    @Override
    public int numColumns() {
        return options.fields()
                .stream()
                .map(ObjectField::options)
                .map(this::mixin)
                .mapToInt(Mixin::numColumns)
                .sum();
    }

    @Override
    public Stream<List<String>> paths() {
        return prefixWithKeys(options.fields());
    }

    @Override
    public ValueProcessor processor(String context) {
        final Map<ObjectField, ValueProcessor> processors = new LinkedHashMap<>(options.fields().size());
        int ix = 0;
        for (ObjectField field : options.fields()) {
            final Mixin<?> opts = mixin(field.options());
            final int numTypes = opts.numColumns();
            final ValueProcessor fieldProcessor = opts.processor(context + "/" + field.name());
            processors.put(field, fieldProcessor);
            ix += numTypes;
        }
        if (ix != numColumns()) {
            throw new IllegalStateException();
        }
        return processorImpl(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        final Map<ObjectField, RepeaterProcessor> processors = new LinkedHashMap<>(options.fields().size());
        int ix = 0;
        for (ObjectField field : options.fields()) {
            final Mixin<?> opts = mixin(field.options());
            final int numTypes = opts.numColumns();
            final RepeaterProcessor fieldProcessor =
                    opts.repeaterProcessor(allowMissing, allowNull);
            processors.put(field, fieldProcessor);
            ix += numTypes;
        }
        if (ix != numColumns()) {
            throw new IllegalStateException();
        }
        return new ObjectValueRepeaterProcessor(processors);
    }

    private boolean allCaseSensitive() {
        return options.fields().stream().allMatch(ObjectField::caseSensitive);
    }

    ObjectValueFieldProcessor processorImpl(Map<ObjectField, ValueProcessor> fields) {
        return new ObjectValueFieldProcessor(fields);
    }

    final class ObjectValueFieldProcessor extends ContextAwareDelegateBase implements ValueProcessor, FieldProcessor {
        private final Map<ObjectField, ValueProcessor> fields;
        private final Map<String, ObjectField> map;
        private final Set<ObjectField> visited;

        ObjectValueFieldProcessor(Map<ObjectField, ValueProcessor> fields) {
            super(fields.values());
            this.fields = fields;
            this.map = allCaseSensitive()
                    ? new HashMap<>()
                    : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Entry<ObjectField, ValueProcessor> e : fields.entrySet()) {
                final ObjectField field = e.getKey();
                map.put(field.name(), field);
                for (String alias : field.aliases()) {
                    map.put(alias, field);
                }
            }
            this.visited = new HashSet<>(fields.size());
        }

        private ObjectField lookupField(String fieldName) {
            final ObjectField field = map.get(fieldName);
            if (field == null) {
                return null;
            }
            if (!field.caseSensitive()) {
                return field;
            }
            // Need to handle the case where some fields are case-insensitive, but this one is _not_.
            if (field.name().equals(fieldName) || field.aliases().contains(fieldName)) {
                return field;
            }
            return null;
        }

        private ValueProcessor processor(ObjectField options) {
            return Objects.requireNonNull(fields.get(options));
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            // see com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
            // com.fasterxml.jackson.databind.DeserializationContext)
            // for notes on FIELD_NAME
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
                    return;
                case VALUE_NULL:
                    processNullObject(parser);
                    return;
                default:
                    throw Parsing.mismatch(parser, Object.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            for (ValueProcessor value : fields.values()) {
                value.processMissing(parser);
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            if (!allowNull()) {
                throw Parsing.mismatch(parser, Object.class);
            }
            for (ValueProcessor value : fields.values()) {
                value.processCurrentValue(parser);
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // This logic should be equivalent to processObjectFields, but where we know there are no fields
            for (ValueProcessor value : fields.values()) {
                value.processMissing(parser);
            }
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            visited.clear();
            FieldProcessor.processFields(parser, this);
            processMissingFields(parser);
        }

        @Override
        public void process(String fieldName, JsonParser parser) throws IOException {
            final ObjectField field = lookupField(fieldName);
            if (field == null) {
                if (!options.allowUnknownFields()) {
                    throw new IOException(
                            String.format("Unexpected field '%s' and allowUnknownFields == false", fieldName));
                }
                parser.skipChildren();
            } else if (visited.add(field)) {
                // First time seeing field
                processor(field).processCurrentValue(parser);
            } else if (field.repeatedBehavior() == RepeatedBehavior.USE_FIRST) {
                parser.skipChildren();
            } else {
                throw new IOException(
                        String.format("Field '%s' has already been visited and repeatedBehavior == %s", fieldName,
                                field.repeatedBehavior()));
            }
        }

        void processMissingFields(JsonParser parser) throws IOException {
            if (visited.size() == fields.size()) {
                // All fields visited, none missing
                return;
            }
            for (Entry<ObjectField, ValueProcessor> e : fields.entrySet()) {
                if (!visited.contains(e.getKey())) {
                    e.getValue().processMissing(parser);
                }
            }
        }
    }

    final class ObjectValueRepeaterProcessor extends ContextAwareDelegateBase
            implements RepeaterProcessor, Context, FieldProcessor {
        private final Map<ObjectField, RepeaterProcessor> fields;
        private final Map<ObjectField, Context> contexts;
        private final Map<String, ObjectField> map;

        public ObjectValueRepeaterProcessor(Map<ObjectField, RepeaterProcessor> fields) {
            super(fields.values());
            this.fields = Objects.requireNonNull(fields);
            contexts = new LinkedHashMap<>(fields.size());
            for (Entry<ObjectField, RepeaterProcessor> e : fields.entrySet()) {
                contexts.put(e.getKey(), e.getValue().context());
            }
            this.map = allCaseSensitive()
                    ? new HashMap<>()
                    : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Entry<ObjectField, RepeaterProcessor> e : fields.entrySet()) {
                final ObjectField field = e.getKey();
                map.put(field.name(), field);
                for (String alias : field.aliases()) {
                    map.put(alias, field);
                }
            }
            this.visited = new HashSet<>(fields.size());
        }

        private Collection<RepeaterProcessor> processors() {
            return fields.values();
        }

        @Override
        public Context context() {
            return this;
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {
            for (RepeaterProcessor p : processors()) {
                p.processNullRepeater(parser);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            for (RepeaterProcessor p : processors()) {
                p.processMissingRepeater(parser);
            }
        }

        private ObjectField lookupField(String fieldName) {
            final ObjectField field = map.get(fieldName);
            if (field == null) {
                return null;
            }
            if (!field.caseSensitive()) {
                return field;
            }
            // Need to handle the case where some fields are case-insensitive, but this one is _not_.
            if (field.name().equals(fieldName) || field.aliases().contains(fieldName)) {
                return field;
            }
            return null;
        }

        private Context context(ObjectField o) {
            return Objects.requireNonNull(contexts.get(o));
        }

        @Override
        public void start(JsonParser parser) throws IOException {
            for (Context value : contexts.values()) {
                value.start(parser);
            }
        }

        @Override
        public void processElement(JsonParser parser) throws IOException {
            // see
            // com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
            // com.fasterxml.jackson.databind.DeserializationContext)
            // for notes on FIELD_NAME
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (parser.nextToken() == JsonToken.END_OBJECT) {
                        processEmptyObject(parser);
                        break;
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

        private void processNullObject(JsonParser parser) throws IOException {
            // element is null
            // pass-through JsonToken.VALUE_NULL
            for (Context context : contexts.values()) {
                context.processElement(parser);
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // This logic should be equivalent to processObjectFields, but where we know there are no fields
            for (Context context : contexts.values()) {
                context.processElementMissing(parser);
            }
        }

        private void processObjectFields(JsonParser parser) throws IOException {
            visited.clear();
            FieldProcessor.processFields(parser, this);
            processMissingFields(parser);
        }

        // -----------------------------------------------------------------------------------------------------------

        private final Set<ObjectField> visited;

        @Override
        public void process(String fieldName, JsonParser parser) throws IOException {
            final ObjectField field = lookupField(fieldName);
            if (field == null) {
                if (!options.allowUnknownFields()) {
                    throw new IOException(
                            String.format("Unexpected field '%s' and allowUnknownFields == false", fieldName));
                }
                parser.skipChildren();
            } else if (visited.add(field)) {
                // First time seeing field
                context(field).processElement(parser);
            } else if (field.repeatedBehavior() == RepeatedBehavior.USE_FIRST) {
                parser.skipChildren();
            } else {
                throw new IOException(
                        String.format("Field '%s' has already been visited and repeatedBehavior == %s",
                                fieldName, field.repeatedBehavior()));
            }
        }

        void processMissingFields(JsonParser parser) throws IOException {
            if (visited.size() == fields.size()) {
                // All fields visited, none missing
                return;
            }
            for (Entry<ObjectField, Context> e : contexts.entrySet()) {
                if (!visited.contains(e.getKey())) {
                    e.getValue().processElementMissing(parser);
                }
            }
        }

        // -----------------------------------------------------------------------------------------------------------

        @Override
        public void processElementMissing(JsonParser parser) throws IOException {
            for (Context context : contexts.values()) {
                context.processElementMissing(parser);
            }
        }

        @Override
        public void done(JsonParser parser) throws IOException {
            for (Context context : contexts.values()) {
                context.done(parser);
            }
        }
    }
}
