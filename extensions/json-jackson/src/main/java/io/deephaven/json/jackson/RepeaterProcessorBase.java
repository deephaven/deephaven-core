//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.jackson.RepeaterProcessor.Context;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

abstract class RepeaterProcessorBase<T> implements RepeaterProcessor, Context {

    private final T onMissing;
    private final T onNull;

    private final NativeArrayType<T, ?> arrayType;

    private WritableObjectChunk<? super T, ?> out;
    private int ix;

    public RepeaterProcessorBase(T onMissing, T onNull, NativeArrayType<T, ?> arrayType) {
        this.onMissing = onMissing;
        this.onNull = onNull;
        this.arrayType = Objects.requireNonNull(arrayType);
    }

    public void startImpl(JsonParser parser) throws IOException {}

    public abstract void processElementImpl(JsonParser parser, int index) throws IOException;

    public abstract void processElementMissingImpl(JsonParser parser, int index) throws IOException;

    public abstract T doneImpl(JsonParser parser, int length) throws IOException;

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
    public Stream<Type<?>> columnTypes() {
        return Stream.of(arrayType);
    }

    @Override
    public final Context context() {
        return this;
    }

    @Override
    public final void processMissingRepeater(JsonParser parser) throws IOException {
        out.add(onMissing);
    }

    @Override
    public final void processNullRepeater(JsonParser parser) throws IOException {
        out.add(onNull);
    }

    @Override
    public final void start(JsonParser parser) throws IOException {
        startImpl(parser);
        ix = 0;
    }

    @Override
    public final void processElement(JsonParser parser) throws IOException {
        processElementImpl(parser, ix);
        ++ix;
    }

    @Override
    public final void processElementMissing(JsonParser parser) throws IOException {
        processElementMissingImpl(parser, ix);
        ++ix;
    }

    @Override
    public final void done(JsonParser parser) throws IOException {
        out.add(doneImpl(parser, ix));
    }
}
