//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ObjectFieldOptions;
import io.deephaven.json.ObjectFieldOptions.RepeatedBehavior;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.jackson.ValueProcessor.FieldProcess;
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

final class ObjectMixin extends Mixin<ObjectOptions> {

    public ObjectMixin(ObjectOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return options.fields()
                .stream()
                .map(ObjectFieldOptions::options)
                .map(this::mixin)
                .flatMap(Mixin::outputTypes);
    }

    @Override
    public int numColumns() {
        return options.fields()
                .stream()
                .map(ObjectFieldOptions::options)
                .map(this::mixin)
                .mapToInt(Mixin::numColumns)
                .sum();
    }

    @Override
    public Stream<List<String>> paths() {
        return prefixWithKeys(options.fields());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final Map<ObjectFieldOptions, ValueProcessor> processors = new LinkedHashMap<>(options.fields().size());
        int ix = 0;
        for (ObjectFieldOptions field : options.fields()) {
            final Mixin<?> opts = mixin(field.options());
            final int numTypes = opts.numColumns();
            final ValueProcessor fieldProcessor =
                    opts.processor(context + "/" + field.name(), out.subList(ix, ix + numTypes));
            processors.put(field, fieldProcessor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return processorImpl(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final Map<ObjectFieldOptions, RepeaterProcessor> processors =
                new LinkedHashMap<>(options.fields().size());
        int ix = 0;

        for (ObjectFieldOptions field : options.fields()) {
            final Mixin<?> opts = mixin(field.options());
            final int numTypes = opts.numColumns();
            final RepeaterProcessor fieldProcessor =
                    opts.repeaterProcessor(allowMissing, allowNull, out.subList(ix, ix + numTypes));
            processors.put(field, fieldProcessor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new ObjectValueRepeaterProcessor(processors);
    }

    private boolean anyCaseInsensitive() {
        return options.fields().stream().anyMatch(ObjectFieldOptions::caseInsensitiveMatch);
    }

    ObjectValueFieldProcessor processorImpl(Map<ObjectFieldOptions, ValueProcessor> fields) {
        return new ObjectValueFieldProcessor(fields);
    }

    final class ObjectValueFieldProcessor implements ValueProcessor {
        private final Map<ObjectFieldOptions, ValueProcessor> fields;
        private final Map<String, ObjectFieldOptions> map;

        ObjectValueFieldProcessor(Map<ObjectFieldOptions, ValueProcessor> fields) {
            this.fields = fields;
            // If _any_ fields are case-insensitive, we add all to TreeMap regardless.
            this.map = anyCaseInsensitive()
                    ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
                    : new HashMap<>();
            for (Entry<ObjectFieldOptions, ValueProcessor> e : fields.entrySet()) {
                final ObjectFieldOptions field = e.getKey();
                map.put(field.name(), field);
                for (String alias : field.aliases()) {
                    map.put(alias, field);
                }
            }
        }

        private ObjectFieldOptions lookupField(String fieldName) {
            final ObjectFieldOptions field = map.get(fieldName);
            if (field == null) {
                return null;
            }
            if (field.caseInsensitiveMatch()) {
                return field;
            }
            // Need to handle the case where some fields are case-insensitive, but this one is _not_.
            if (field.name().equals(fieldName) || field.aliases().contains(fieldName)) {
                return field;
            }
            return null;
        }

        private ValueProcessor processor(ObjectFieldOptions options) {
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
            if (!options.allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            for (ValueProcessor value : fields.values()) {
                value.processMissing(parser);
            }
        }

        private void processNullObject(JsonParser parser) throws IOException {
            if (!options.allowNull()) {
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
            final State state = new State();
            ValueProcessor.processFields(parser, state);
            state.processMissing(parser);
        }

        private class State implements FieldProcess {
            // Note: we could try to build a stricter implementation that doesn't use Set; if the user can guarantee
            // that none of the fields will be missing and there won't be any repeated fields, we could use a simple
            // counter to ensure all field processors were invoked.
            private final Set<ObjectFieldOptions> visited = new HashSet<>(fields.size());

            @Override
            public void process(String fieldName, JsonParser parser) throws IOException {
                final ObjectFieldOptions field = lookupField(fieldName);
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

            void processMissing(JsonParser parser) throws IOException {
                for (Entry<ObjectFieldOptions, ValueProcessor> e : fields.entrySet()) {
                    if (!visited.contains(e.getKey())) {
                        e.getValue().processMissing(parser);
                    }
                }
            }
        }
    }

    final class ObjectValueRepeaterProcessor implements RepeaterProcessor {
        private final Map<ObjectFieldOptions, RepeaterProcessor> fields;

        public ObjectValueRepeaterProcessor(Map<ObjectFieldOptions, RepeaterProcessor> fields) {
            this.fields = Objects.requireNonNull(fields);
        }

        private Collection<RepeaterProcessor> processors() {
            return fields.values();
        }

        @Override
        public Context start(JsonParser parser) throws IOException {
            final Map<ObjectFieldOptions, Context> contexts = new LinkedHashMap<>(fields.size());
            for (Entry<ObjectFieldOptions, RepeaterProcessor> e : fields.entrySet()) {
                contexts.put(e.getKey(), e.getValue().start(parser));
            }
            return new ObjectArrayContext(contexts);
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

        final class ObjectArrayContext implements Context {
            private final Map<ObjectFieldOptions, Context> contexts;
            private final Map<String, ObjectFieldOptions> map;

            public ObjectArrayContext(Map<ObjectFieldOptions, Context> contexts) {
                this.contexts = Objects.requireNonNull(contexts);
                // If _any_ fields are case-insensitive, we add all to TreeMap regardless.
                this.map = anyCaseInsensitive()
                        ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
                        : new HashMap<>();
                for (Entry<ObjectFieldOptions, RepeaterProcessor> e : fields.entrySet()) {
                    final ObjectFieldOptions field = e.getKey();
                    map.put(field.name(), field);
                    for (String alias : field.aliases()) {
                        map.put(alias, field);
                    }
                }
            }

            private ObjectFieldOptions lookupField(String fieldName) {
                final ObjectFieldOptions field = map.get(fieldName);
                if (field == null) {
                    return null;
                }
                if (field.caseInsensitiveMatch()) {
                    return field;
                }
                // Need to handle the case where some fields are case-insensitive, but this one is _not_.
                if (field.name().equals(fieldName) || field.aliases().contains(fieldName)) {
                    return field;
                }
                return null;
            }

            private Context context(ObjectFieldOptions o) {
                return Objects.requireNonNull(contexts.get(o));
            }

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                // see
                // com.fasterxml.jackson.databind.JsonDeserializer.deserialize(com.fasterxml.jackson.core.JsonParser,
                // com.fasterxml.jackson.databind.DeserializationContext)
                // for notes on FIELD_NAME
                switch (parser.currentToken()) {
                    case START_OBJECT:
                        if (parser.nextToken() == JsonToken.END_OBJECT) {
                            processEmptyObject(parser, index);
                            return;
                        }
                        if (!parser.hasToken(JsonToken.FIELD_NAME)) {
                            throw new IllegalStateException();
                        }
                        // fall-through
                    case FIELD_NAME:
                        processObjectFields(parser, index);
                        return;
                    case VALUE_NULL:
                        processNullObject(parser, index);
                        return;
                    default:
                        throw Parsing.mismatch(parser, Object.class);
                }
            }

            private void processNullObject(JsonParser parser, int ix) throws IOException {
                // element is null
                // pass-through JsonToken.VALUE_NULL
                for (Context context : contexts.values()) {
                    context.processElement(parser, ix);
                }
            }

            private void processEmptyObject(JsonParser parser, int ix) throws IOException {
                // This logic should be equivalent to processObjectFields, but where we know there are no fields
                for (Context context : contexts.values()) {
                    context.processElementMissing(parser, ix);
                }
            }

            private void processObjectFields(JsonParser parser, int ix) throws IOException {
                final State state = new State(ix);
                ValueProcessor.processFields(parser, state);
                state.processMissing(parser);
            }

            private class State implements FieldProcess {
                // Note: we could try to build a stricter implementation that doesn't use Set; if the user can guarantee
                // that none of the fields will be missing and there won't be any repeated fields, we could use a simple
                // counter to ensure all field processors were invoked.
                private final Set<ObjectFieldOptions> visited = new HashSet<>(contexts.size());
                private final int ix;

                public State(int ix) {
                    this.ix = ix;
                }

                @Override
                public void process(String fieldName, JsonParser parser) throws IOException {
                    final ObjectFieldOptions field = lookupField(fieldName);
                    if (field == null) {
                        if (!options.allowUnknownFields()) {
                            throw new IOException(
                                    String.format("Unexpected field '%s' and allowUnknownFields == false", fieldName));
                        }
                        parser.skipChildren();
                    } else if (visited.add(field)) {
                        // First time seeing field
                        context(field).processElement(parser, ix);
                    } else if (field.repeatedBehavior() == RepeatedBehavior.USE_FIRST) {
                        parser.skipChildren();
                    } else {
                        throw new IOException(
                                String.format("Field '%s' has already been visited and repeatedBehavior == %s",
                                        fieldName, field.repeatedBehavior()));
                    }
                }

                void processMissing(JsonParser parser) throws IOException {
                    for (Entry<ObjectFieldOptions, Context> e : contexts.entrySet()) {
                        if (!visited.contains(e.getKey())) {
                            e.getValue().processElementMissing(parser, ix);
                        }
                    }
                }
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                for (Context context : contexts.values()) {
                    context.processElementMissing(parser, index);
                }
            }

            @Override
            public void done(JsonParser parser, int length) throws IOException {
                for (Context context : contexts.values()) {
                    context.done(parser, length);
                }
            }
        }
    }
}
