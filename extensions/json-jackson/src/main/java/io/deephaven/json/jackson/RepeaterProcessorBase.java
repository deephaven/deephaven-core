//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

abstract class RepeaterProcessorBase<T> implements RepeaterProcessor {

    private final Consumer<? super T> consumer;
    private final boolean allowMissing;
    private final boolean allowNull;
    private final T onMissing;
    private final T onNull;

    public RepeaterProcessorBase(Consumer<? super T> consumer, boolean allowMissing, boolean allowNull, T onMissing,
            T onNull) {
        this.consumer = Objects.requireNonNull(consumer);
        this.onMissing = onMissing;
        this.onNull = onNull;
        this.allowNull = allowNull;
        this.allowMissing = allowMissing;
    }

    public abstract RepeaterContextBase newContext();

    @Override
    public final Context start(JsonParser parser) throws IOException {
        return newContext();
    }

    @Override
    public final void processMissingRepeater(JsonParser parser) throws IOException {
        if (!allowMissing) {
            throw Parsing.mismatchMissing(parser, void.class);
        }
        consumer.accept(onMissing);
    }

    @Override
    public final void processNullRepeater(JsonParser parser) throws IOException {
        if (!allowNull) {
            throw Parsing.mismatch(parser, void.class);
        }
        consumer.accept(onNull);
    }

    public abstract class RepeaterContextBase implements Context {

        @Override
        public final void done(JsonParser parser, int length) throws IOException {
            consumer.accept(onDone(length));
        }

        public abstract T onDone(int length);
    }
}
