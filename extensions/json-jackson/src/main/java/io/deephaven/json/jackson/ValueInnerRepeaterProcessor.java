//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.sized.SizedObjectChunk;
import io.deephaven.json.jackson.RepeaterProcessor.Context;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

final class ValueInnerRepeaterProcessor implements RepeaterProcessor, Context {

    private final ValueProcessor innerProcessor;
    private final List<WritableObjectChunk<?, ?>> innerChunks;
    private final List<SizedObjectChunk<?, ?>> sizedObjectChunks;

    private int ix;

    private List<WritableObjectChunk<Object[], ?>> out;

    public ValueInnerRepeaterProcessor(ValueProcessor innerProcessor) {
        final List<WritableChunk<?>> innerChunks = innerProcessor.columnTypes()
                .map(Type::arrayType)
                .map(ObjectProcessor::chunkType)
                .peek(ValueInnerRepeaterProcessor::checkObjectChunk)
                .map(o -> o.makeWritableChunk(1))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
        // we _know_ these are all object chunks, given NativeArrayType
        // noinspection unchecked,rawtypes
        this.innerChunks = (List<WritableObjectChunk<?, ?>>) (List) innerChunks;
        this.innerProcessor = innerProcessor;
        this.innerProcessor.setContext(innerChunks);
        sizedObjectChunks = IntStream.range(0, innerProcessor.numColumns())
                .mapToObj(i -> new SizedObjectChunk<>(0))
                .collect(Collectors.toList());
    }

    static void checkObjectChunk(ChunkType chunkType) {
        if (chunkType != ChunkType.Object) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        // noinspection unchecked,rawtypes
        this.out = (List<WritableObjectChunk<Object[], ?>>) (List) Objects.requireNonNull(out);
    }

    @Override
    public void clearContext() {
        out = null;
    }

    @Override
    public int numColumns() {
        return sizedObjectChunks.size();
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return innerProcessor.columnTypes().map(Type::arrayType);
    }

    @Override
    public void processNullRepeater(JsonParser parser) throws IOException {
        for (WritableObjectChunk<?, ?> wc : out) {
            wc.add(null);
        }
    }

    @Override
    public void processMissingRepeater(JsonParser parser) throws IOException {
        for (WritableObjectChunk<?, ?> wc : out) {
            wc.add(null);
        }
    }

    @Override
    public Context context() {
        return this;
    }

    @Override
    public void start(JsonParser parser) throws IOException {
        // Note: we are setting the context once up-front
        // innerProcessor.setContext(innerChunks);
        ix = 0;
    }

    @Override
    public void processElement(JsonParser parser) throws IOException {
        innerProcessor.processCurrentValue(parser);
        processImpl();
    }

    @Override
    public void processElementMissing(JsonParser parser) throws IOException {
        innerProcessor.processMissing(parser);
        processImpl();
    }

    private void processImpl() {
        final int newSize = ix + 1;
        final int L = sizedObjectChunks.size();
        for (int i = 0; i < L; ++i) {
            // noinspection unchecked
            final WritableObjectChunk<Object, ?> from = (WritableObjectChunk<Object, ?>) innerChunks.get(i);
            // noinspection unchecked
            final SizedObjectChunk<Object, ?> to = (SizedObjectChunk<Object, ?>) sizedObjectChunks.get(i);
            // we _could_ consider doing this in a chunked fashion. doing in simple fashion to initially test
            to.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            to.get().set(ix, from.get(0));
            to.get().setSize(newSize);
            from.set(0, null);
            from.setSize(0);
        }
        ix = newSize;
    }

    @Override
    public void done(JsonParser parser) throws IOException {
        // Note: we are setting the context once up-front
        // innerProcessor.clearContext();
        final int L = sizedObjectChunks.size();
        for (int i = 0; i < L; ++i) {
            final WritableObjectChunk<?, ?> from = sizedObjectChunks.get(i).get();
            final Object[] objects = Arrays.copyOfRange(from.array(), from.arrayOffset(), from.arrayOffset() + ix);
            from.fillWithNullValue(0, ix);
            out.get(i).add(objects);
        }
    }
}
