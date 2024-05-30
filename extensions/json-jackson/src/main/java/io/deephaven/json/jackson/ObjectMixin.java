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
import java.util.Collections;
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

    private final Map<ObjectField, Mixin<?>> mixins;
    private final int numOutputs;

    public ObjectMixin(ObjectValue options, JsonFactory factory) {
        super(factory, options);
        final LinkedHashMap<ObjectField, Mixin<?>> map = new LinkedHashMap<>(options.fields().size());
        for (ObjectField field : options.fields()) {
            map.put(field, Mixin.of(field.options(), factory));
        }
        mixins = Collections.unmodifiableMap(map);
        numOutputs = mixins.values().stream().mapToInt(Mixin::outputSize).sum();
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return mixins.values().stream().flatMap(Mixin::outputTypesImpl);
    }

    @Override
    public int outputSize() {
        return numOutputs;
    }

    @Override
    public Stream<List<String>> paths() {
        return prefixWithKeys(mixins);
    }

    @Override
    public ValueProcessor processor(String context) {
        return processor(context, false);
    }

    public ValueProcessor processor(String context, boolean isDiscriminated) {
        final Map<ObjectField, ValueProcessor> processors = new LinkedHashMap<>(mixins.size());
        int ix = 0;
        for (Entry<ObjectField, Mixin<?>> e : mixins.entrySet()) {
            final ObjectField field = e.getKey();
            final Mixin<?> opts = e.getValue();
            final int numTypes = opts.outputSize();
            final ValueProcessor fieldProcessor = opts.processor(context + "/" + field.name());
            processors.put(field, fieldProcessor);
            ix += numTypes;
        }
        if (ix != outputSize()) {
            throw new IllegalStateException();
        }
        return processorImpl(processors, isDiscriminated);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        final Map<ObjectField, RepeaterProcessor> processors = new LinkedHashMap<>(mixins.size());
        int ix = 0;
        for (Entry<ObjectField, Mixin<?>> e : mixins.entrySet()) {
            final ObjectField field = e.getKey();
            final Mixin<?> opts = e.getValue();
            final int numTypes = opts.outputSize();
            final RepeaterProcessor fieldProcessor = opts.repeaterProcessor();
            processors.put(field, fieldProcessor);
            ix += numTypes;
        }
        if (ix != outputSize()) {
            throw new IllegalStateException();
        }
        return new ObjectValueRepeaterProcessor(processors);
    }

    private boolean allCaseSensitive() {
        return options.fields().stream().allMatch(ObjectField::caseSensitive);
    }

    ObjectValueFieldProcessor processorImpl(Map<ObjectField, ValueProcessor> fields, boolean isDiscriminatedObject) {
        return new ObjectValueFieldProcessor(fields, isDiscriminatedObject);
    }

    final class ObjectValueFieldProcessor extends ContextAwareDelegateBase implements ValueProcessor, FieldProcessor {
        private final Map<ObjectField, ValueProcessor> fields;
        private final Map<String, ObjectField> map;
        private final boolean isDiscriminated;

        ObjectValueFieldProcessor(Map<ObjectField, ValueProcessor> fields, boolean isDiscriminated) {
            super(fields.values());
            this.fields = fields;
            this.map = allCaseSensitive()
                    ? new HashMap<>()
                    : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            this.isDiscriminated = isDiscriminated;
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
            // In the normal case, we expect to be at START_OBJECT or VALUE_NULL.
            // In the discriminated case, we expect to already be inside an object (FIELD_NAME or END_OBJECT).
            switch (parser.currentToken()) {
                case START_OBJECT:
                    if (isDiscriminated) {
                        throw unexpectedToken(parser);
                    }
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
                    if (isDiscriminated) {
                        throw unexpectedToken(parser);
                    }
                    processNullObject(parser);
                    return;
                case FIELD_NAME:
                    if (!isDiscriminated) {
                        throw unexpectedToken(parser);
                    }
                    processObjectFields(parser);
                    return;
                case END_OBJECT:
                    if (!isDiscriminated) {
                        throw unexpectedToken(parser);
                    }
                    processEmptyObject(parser);
                    return;
                default:
                    throw unexpectedToken(parser);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            for (Entry<ObjectField, ValueProcessor> entry : fields.entrySet()) {
                processMissingField(entry.getKey(), entry.getValue(), parser);
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            checkNullAllowed(parser);
            for (Entry<ObjectField, ValueProcessor> entry : fields.entrySet()) {
                processField(entry.getKey(), entry.getValue(), parser);
            }
        }

        private void processEmptyObject(JsonParser parser) throws IOException {
            // This logic should be equivalent to processObjectFields, but where we know there are no fields
            for (Entry<ObjectField, ValueProcessor> entry : fields.entrySet()) {
                processMissingField(entry.getKey(), entry.getValue(), parser);
            }
        }

        // -----------------------------------------------------------------------------------------------------------

        private final Set<ObjectField> visited;

        private void processObjectFields(JsonParser parser) throws IOException {
            try {
                FieldProcessor.processFields(parser, this);
                if (visited.size() == fields.size()) {
                    // All fields visited, none missing
                    return;
                }
                for (Entry<ObjectField, ValueProcessor> e : fields.entrySet()) {
                    if (!visited.contains(e.getKey())) {
                        processMissingField(e.getKey(), e.getValue(), parser);
                    }
                }
            } finally {
                visited.clear();
            }
        }

        @Override
        public void process(String fieldName, JsonParser parser) throws IOException {
            final ObjectField field = lookupField(fieldName);
            if (field == null) {
                if (!options.allowUnknownFields()) {
                    throw new ValueAwareException(String.format("Unknown field '%s' not allowed", fieldName),
                            parser.currentLocation(), options);
                }
                parser.skipChildren();
            } else if (visited.add(field)) {
                // First time seeing field
                processField(field, processor(field), parser);
            } else if (field.repeatedBehavior() == RepeatedBehavior.USE_FIRST) {
                parser.skipChildren();
            } else {
                throw new ValueAwareException(String.format("Field '%s' has already been visited", fieldName),
                        parser.currentLocation(), options);
            }
        }

        // -----------------------------------------------------------------------------------------------------------

        private void processField(ObjectField field, ValueProcessor processor, JsonParser parser)
                throws ValueAwareException {
            try {
                processor.processCurrentValue(parser);
            } catch (IOException | RuntimeException e) {
                throw new ValueAwareException(String.format("Unable to process field '%s'", field.name()),
                        parser.currentLocation(), e, options);
            }
        }

        private void processMissingField(ObjectField field, ValueProcessor processor, JsonParser parser)
                throws ValueAwareException {
            try {
                processor.processMissing(parser);
            } catch (IOException | RuntimeException e) {
                throw new ValueAwareException(String.format("Unable to process field '%s'", field.name()),
                        parser.currentLocation(), e, options);
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
            // Not supporting TypedObjectValue array (TypedObjectMixin#repeaterProcessor) yet, so don't need
            // discrimination support here.
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

        // -----------------------------------------------------------------------------------------------------------

        private final Set<ObjectField> visited;

        private void processObjectFields(JsonParser parser) throws IOException {
            try {
                FieldProcessor.processFields(parser, this);
                if (visited.size() == fields.size()) {
                    // All fields visited, none missing
                    return;
                }
                for (Entry<ObjectField, Context> e : contexts.entrySet()) {
                    if (!visited.contains(e.getKey())) {
                        e.getValue().processElementMissing(parser);
                    }
                }
            } finally {
                visited.clear();
            }
        }

        @Override
        public void process(String fieldName, JsonParser parser) throws IOException {
            final ObjectField field = lookupField(fieldName);
            if (field == null) {
                if (!options.allowUnknownFields()) {
                    throw new ValueAwareException(
                            String.format("Unexpected field '%s' and allowUnknownFields == false", fieldName),
                            parser.currentLocation(), options);
                }
                parser.skipChildren();
            } else if (visited.add(field)) {
                // First time seeing field
                context(field).processElement(parser);
            } else if (field.repeatedBehavior() == RepeatedBehavior.USE_FIRST) {
                parser.skipChildren();
            } else {
                throw new ValueAwareException(
                        String.format("Field '%s' has already been visited and repeatedBehavior == %s", fieldName,
                                field.repeatedBehavior()),
                        parser.currentLocation(), options);
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
