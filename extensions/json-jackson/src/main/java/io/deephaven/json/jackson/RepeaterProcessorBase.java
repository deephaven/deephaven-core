//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.jackson.RepeaterProcessor.Context;

import java.io.IOException;
import java.util.List;

abstract class RepeaterProcessorBase<T> implements RepeaterProcessor, Context {

    private final boolean allowMissing;
    private final boolean allowNull;
    private final T onMissing;
    private final T onNull;

    private WritableObjectChunk<? super T, ?> out;

    public RepeaterProcessorBase(boolean allowMissing, boolean allowNull, T onMissing, T onNull) {
        this.onMissing = onMissing;
        this.onNull = onNull;
        this.allowNull = allowNull;
        this.allowMissing = allowMissing;
    }

    public abstract T doneImpl(JsonParser parser, int length);

    @Override
    public final void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableObjectChunk();
    }

    @Override
    public final void clearContext() {
        out = null;
    }

    @Override
    public final int numColumns() {
        return 1;
    }

    @Override
    public final Context context() {
        return this;
    }

    @Override
    public final void processMissingRepeater(JsonParser parser) throws IOException {
        if (!allowMissing) {
            throw Parsing.mismatchMissing(parser, void.class);
        }
        out.add(onMissing);
    }

    @Override
    public final void processNullRepeater(JsonParser parser) throws IOException {
        if (!allowNull) {
            throw Parsing.mismatch(parser, void.class);
        }
        out.add(onNull);
    }

    @Override
    public final void init(JsonParser parser) throws IOException {

    }

    @Override
    public final void done(JsonParser parser, int length) throws IOException {
        out.add(doneImpl(parser, length));
    }
}
