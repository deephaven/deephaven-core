//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.deephaven.json.jackson.Parsing.assertCurrentToken;

final class TupleMixin extends Mixin<TupleOptions> {

    public TupleMixin(TupleOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
        return options.values().stream().map(this::mixin).mapToInt(Mixin::numColumns).sum();
    }

    @Override
    public Stream<List<String>> paths() {
        if (options.values().size() == 1) {
            return mixin(options.values().get(0)).paths();
        }
        // todo, give user naming option?
        final List<Stream<List<String>>> prefixed = new ArrayList<>();
        int i = 0;
        for (ValueOptions value : options.values()) {
            final int ix = i;
            prefixed.add(mixin(value).paths().map(x -> prefixWith(String.valueOf(ix), x)));
            ++i;
        }
        return prefixed.stream().flatMap(Function.identity());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return options.values().stream().map(this::mixin).flatMap(Mixin::outputTypes);
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final List<ValueProcessor> processors = new ArrayList<>(options.values().size());
        int ix = 0;
        int processorIx = 0;
        for (ValueOptions value : options.values()) {
            final Mixin<?> mixin = mixin(value);
            final int numTypes = mixin.numColumns();
            final ValueProcessor processor =
                    mixin.processor(context + "[" + processorIx + "]", out.subList(ix, ix + numTypes));
            processors.add(processor);
            ix += numTypes;
            ++processorIx;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new TupleProcessor(processors);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        if (out.size() != numColumns()) {
            throw new IllegalArgumentException();
        }
        final List<RepeaterProcessor> processors = new ArrayList<>(options.values().size());
        int ix = 0;
        for (ValueOptions value : options.values()) {
            final Mixin<?> mixin = mixin(value);
            final int numTypes = mixin.numColumns();
            final RepeaterProcessor processor =
                    mixin.repeaterProcessor(allowMissing, allowNull, out.subList(ix, ix + numTypes));
            processors.add(processor);
            ix += numTypes;
        }
        if (ix != out.size()) {
            throw new IllegalStateException();
        }
        return new TupleArrayProcessor(processors);
    }

    private class TupleProcessor implements ValueProcessor {
        private final List<ValueProcessor> values;

        public TupleProcessor(List<ValueProcessor> values) {
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
            if (!options.allowNull()) {
                throw Parsing.mismatch(parser, Object.class);
            }
            // Note: we are treating a null tuple the same as a tuple of null objects
            // null ~= [null, ..., null]
            for (ValueProcessor value : values) {
                value.processCurrentValue(parser);
            }
        }

        private void parseFromMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
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

    final class TupleArrayProcessor implements RepeaterProcessor {
        private final List<RepeaterProcessor> values;

        public TupleArrayProcessor(List<RepeaterProcessor> values) {
            this.values = Objects.requireNonNull(values);
        }

        @Override
        public TupleArrayContext start(JsonParser parser) throws IOException {
            final List<Context> contexts = new ArrayList<>(values.size());
            for (RepeaterProcessor value : values) {
                contexts.add(value.start(parser));
            }
            return new TupleArrayContext(contexts);
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Parsing.mismatch(parser, Object.class);
            }
            for (RepeaterProcessor value : values) {
                value.processNullRepeater(parser);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            for (RepeaterProcessor value : values) {
                value.processMissingRepeater(parser);
            }
        }

        final class TupleArrayContext implements Context {
            private final List<Context> contexts;

            public TupleArrayContext(List<Context> contexts) {
                this.contexts = Objects.requireNonNull(contexts);
            }

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
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
                    context.processElement(parser, index);
                }
                parser.nextToken();
                assertCurrentToken(parser, JsonToken.END_ARRAY);
            }

            private void processNullTuple(JsonParser parser, int index) throws IOException {
                // Note: we are treating a null tuple the same as a tuple of null objects
                // null ~= [null, ..., null]
                for (Context context : contexts) {
                    context.processElement(parser, index);
                }
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                // Note: we are treating a missing tuple the same as a tuple of missing objects (which, is technically
                // impossible w/ native json, but it's the semantics we are exposing).
                // <missing> ~= [<missing>, ..., <missing>]
                for (Context context : contexts) {
                    context.processElementMissing(parser, index);
                }
            }

            @Override
            public void done(JsonParser parser, int length) throws IOException {
                for (Context context : contexts) {
                    context.done(parser, length);
                }
            }
        }
    }
}
