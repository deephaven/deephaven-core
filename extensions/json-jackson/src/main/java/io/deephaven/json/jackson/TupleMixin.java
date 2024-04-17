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
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.deephaven.json.jackson.Parsing.assertCurrentToken;

final class TupleMixin extends Mixin<TupleValue> {

    public TupleMixin(TupleValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
        return options.namedValues().values().stream().map(this::mixin).mapToInt(Mixin::numColumns).sum();
    }

    @Override
    public Stream<List<String>> paths() {
        if (options.namedValues().size() == 1) {
            return mixin(options.namedValues().get(0)).paths();
        }
        final List<Stream<List<String>>> prefixed = new ArrayList<>();
        for (Entry<String, Value> e : options.namedValues().entrySet()) {
            prefixed.add(mixin(e.getValue()).paths().map(x -> prefixWith(e.getKey(), x)));
        }
        return prefixed.stream().flatMap(Function.identity());
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return options.namedValues().values().stream().map(this::mixin).flatMap(Mixin::outputTypesImpl);
    }

    @Override
    public ValueProcessor processor(String context) {
        final List<ValueProcessor> processors = new ArrayList<>(options.namedValues().size());
        int ix = 0;
        for (Entry<String, Value> e : options.namedValues().entrySet()) {
            final Mixin<?> mixin = mixin(e.getValue());
            final int numTypes = mixin.numColumns();
            final ValueProcessor processor = mixin.processor(context + "[" + e.getKey() + "]");
            processors.add(processor);
            ix += numTypes;
        }
        if (ix != numColumns()) {
            throw new IllegalStateException();
        }
        return new TupleProcessor(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        final List<RepeaterProcessor> processors = new ArrayList<>(options.namedValues().size());
        int ix = 0;
        for (Entry<String, Value> e : options.namedValues().entrySet()) {
            final Mixin<?> mixin = mixin(e.getValue());
            final int numTypes = mixin.numColumns();
            final RepeaterProcessor processor =
                    mixin.repeaterProcessor(allowMissing, allowNull);
            processors.add(processor);
            ix += numTypes;
        }
        if (ix != numColumns()) {
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
                    throw Parsing.mismatch(parser, Object.class);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            parseFromMissing(parser);
        }

        private void processTuple(JsonParser parser) throws IOException {
            for (ValueProcessor value : values) {
                parser.nextToken();
                value.processCurrentValue(parser);
            }
            parser.nextToken();
            assertCurrentToken(parser, JsonToken.END_ARRAY);
        }

        private void processNullTuple(JsonParser parser) throws IOException {
            if (!allowNull()) {
                throw Parsing.mismatch(parser, Object.class);
            }
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (ValueProcessor value : values) {
                value.processCurrentValue(parser);
            }
        }

        private void parseFromMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            // Note: we are treating a missing tuple the same as a tuple of missing objects (which, is technically
            // impossible w/ native json, but it's the semantics we are exposing).
            // <missing> ~= [<missing>, ..., <missing>]
            for (ValueProcessor value : values) {
                value.processMissing(parser);
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
            if (!allowMissing()) {
                throw Parsing.mismatch(parser, Object.class);
            }
            for (RepeaterProcessor value : values) {
                value.processNullRepeater(parser);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
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
                    processTuple(parser, index);
                    return;
                case VALUE_NULL:
                    processNullTuple(parser, index);
                    return;
                default:
                    throw Parsing.mismatch(parser, Object.class);
            }
        }

        private void processTuple(JsonParser parser, int index) throws IOException {
            for (Context context : contexts) {
                parser.nextToken();
                context.processElement(parser);
            }
            parser.nextToken();
            assertCurrentToken(parser, JsonToken.END_ARRAY);
        }

        private void processNullTuple(JsonParser parser, int index) throws IOException {
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (Context context : contexts) {
                context.processElement(parser);
            }
        }

        @Override
        public void processElementMissing(JsonParser parser) throws IOException {
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
