//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.TupleValue;
import io.deephaven.json.Value;
import io.deephaven.json.jackson.RepeaterProcessor.Context;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.deephaven.json.jackson.Parsing.assertCurrentToken;

final class TupleMixin extends Mixin<TupleValue> {

    private final Map<String, Mixin<?>> mixins;
    private final int numColumns;

    public TupleMixin(TupleValue options, JsonFactory factory) {
        super(factory, options);
        final LinkedHashMap<String, Mixin<?>> map = new LinkedHashMap<>(options.namedValues().size());
        for (Entry<String, Value> e : options.namedValues().entrySet()) {
            map.put(e.getKey(), Mixin.of(e.getValue(), factory));
        }
        mixins = Collections.unmodifiableMap(map);
        numColumns = mixins.values().stream().mapToInt(Mixin::outputSize).sum();
    }

    @Override
    public int outputSize() {
        return numColumns;
    }

    @Override
    public Stream<List<String>> paths() {
        if (mixins.size() == 1) {
            return mixins.values().iterator().next().paths();
        }
        final List<Stream<List<String>>> prefixed = new ArrayList<>();
        for (Entry<String, Mixin<?>> e : mixins.entrySet()) {
            prefixed.add(e.getValue().paths().map(x -> prefixWith(e.getKey(), x)));
        }
        return prefixed.stream().flatMap(Function.identity());
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return mixins.values().stream().flatMap(Mixin::outputTypesImpl);
    }

    @Override
    public ValueProcessor processor(String context) {
        final List<ValueProcessor> processors = new ArrayList<>(options.namedValues().size());
        int ix = 0;
        for (Entry<String, Mixin<?>> e : mixins.entrySet()) {
            final Mixin<?> mixin = e.getValue();
            final int numTypes = mixin.outputSize();
            final ValueProcessor processor = mixin.processor(context + "[" + e.getKey() + "]");
            processors.add(processor);
            ix += numTypes;
        }
        if (ix != outputSize()) {
            throw new IllegalStateException();
        }
        return new TupleProcessor(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        final List<RepeaterProcessor> processors = new ArrayList<>(mixins.size());
        int ix = 0;
        for (Entry<String, Mixin<?>> e : mixins.entrySet()) {
            final Mixin<?> mixin = e.getValue();
            final int numTypes = mixin.outputSize();
            final RepeaterProcessor processor = mixin.repeaterProcessor();
            processors.add(processor);
            ix += numTypes;
        }
        if (ix != outputSize()) {
            throw new IllegalStateException();
        }
        return new TupleArrayProcessor(processors);
    }

    private class TupleProcessor extends ContextAwareDelegateBase implements ValueProcessor {
        private final List<ValueProcessor> values;

        public TupleProcessor(List<ValueProcessor> values) {
            super(values);
            this.values = Objects.requireNonNull(values);
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_ARRAY:
                    processTuple(parser);
                    return;
                case VALUE_NULL:
                    processNullTuple(parser);
                    return;
                default:
                    throw unexpectedToken(parser);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            parseFromMissing(parser);
        }

        private void processTuple(JsonParser parser) throws IOException {
            int ix = 0;
            for (ValueProcessor value : values) {
                parser.nextToken();
                try {
                    value.processCurrentValue(parser);
                } catch (IOException | RuntimeException e) {
                    throw new ValueAwareException(String.format("Unable to process tuple ix %d", ix),
                            parser.currentLocation(), e, options);
                }
                ++ix;
            }
            parser.nextToken();
            assertCurrentToken(parser, JsonToken.END_ARRAY);
        }

        private void processNullTuple(JsonParser parser) throws IOException {
            checkNullAllowed(parser);
            int ix = 0;
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (ValueProcessor value : values) {
                // Note: _not_ incrementing to nextToken
                try {
                    value.processCurrentValue(parser);
                } catch (IOException | RuntimeException e) {
                    throw new ValueAwareException(String.format("Unable to process tuple ix %d", ix),
                            parser.currentLocation(), e, options);
                }
                ++ix;
            }
        }

        private void parseFromMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            int ix = 0;
            // Note: we are treating a missing tuple the same as a tuple of missing objects (which, is technically
            // impossible w/ native json, but it's the semantics we are exposing).
            // <missing> ~= [<missing>, ..., <missing>]
            for (ValueProcessor value : values) {
                try {
                    value.processMissing(parser);
                } catch (IOException | RuntimeException e) {
                    throw new ValueAwareException(String.format("Unable to process tuple ix %d", ix),
                            parser.currentLocation(), e, options);
                }
                ++ix;
            }
        }
    }

    final class TupleArrayProcessor extends ContextAwareDelegateBase implements RepeaterProcessor, Context {
        private final List<RepeaterProcessor> values;
        private final List<Context> contexts;

        public TupleArrayProcessor(List<RepeaterProcessor> values) {
            super(values);
            this.values = Objects.requireNonNull(values);
            this.contexts = new ArrayList<>(values.size());
            for (RepeaterProcessor value : values) {
                contexts.add(value.context());
            }
        }

        @Override
        public Context context() {
            return this;
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {
            for (RepeaterProcessor value : values) {
                value.processNullRepeater(parser);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            for (RepeaterProcessor value : values) {
                value.processMissingRepeater(parser);
            }
        }

        @Override
        public void start(JsonParser parser) throws IOException {
            for (Context context : contexts) {
                context.start(parser);
            }
        }

        @Override
        public void processElement(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_ARRAY:
                    processTuple(parser);
                    break;
                case VALUE_NULL:
                    processNullTuple(parser);
                    break;
                default:
                    throw unexpectedToken(parser);
            }
        }

        private void processTuple(JsonParser parser) throws IOException {
            for (Context context : contexts) {
                parser.nextToken();
                context.processElement(parser);
            }
            parser.nextToken();
            assertCurrentToken(parser, JsonToken.END_ARRAY);
        }

        private void processNullTuple(JsonParser parser) throws IOException {
            checkNullAllowed(parser);
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (Context context : contexts) {
                context.processElement(parser);
            }
        }

        @Override
        public void processElementMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            // Note: we are treating a missing tuple the same as a tuple of missing objects (which, is technically
            // impossible w/ native json, but it's the semantics we are exposing).
            // <missing> ~= [<missing>, ..., <missing>]
            for (Context context : contexts) {
                context.processElementMissing(parser);
            }
        }

        @Override
        public void done(JsonParser parser) throws IOException {
            for (Context context : contexts) {
                context.done(parser);
            }
        }
    }
}
