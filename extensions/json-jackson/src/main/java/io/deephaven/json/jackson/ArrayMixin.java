//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.json.ArrayOptions;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ArrayMixin extends Mixin<ArrayOptions> {

    public ArrayMixin(ArrayOptions options, JsonFactory factory) {
        super(factory, options);
    }

    Mixin<?> element() {
        return mixin(options.element());
    }

    @Override
    public int numColumns() {
        return element().numColumns();
    }

    @Override
    public Stream<List<String>> paths() {
        return element().paths();
    }

    @Override
    public Stream<NativeArrayType<?, ?>> outputTypesImpl() {
        return elementOutputTypes().map(Type::arrayType);
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return innerProcessor(out);
    }

    Stream<? extends Type<?>> elementOutputTypes() {
        return element().outputTypesImpl();
    }

    RepeaterProcessor elementRepeater(List<WritableChunk<?>> out) {
        return element().repeaterProcessor(allowMissing(), allowNull(), out);
    }

    private ValueProcessorArrayImpl innerProcessor(List<WritableChunk<?>> out) {
        return new ValueProcessorArrayImpl(elementRepeater(out));
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        // For example:
        // double (element())
        // double[] (processor())
        // double[][] (arrayProcessor())
        return new ArrayOfArrayProcessor(out, allowMissing, allowNull);
    }

    final class ArrayOfArrayProcessor implements RepeaterProcessor {
        private final List<WritableChunk<?>> out;
        private final List<NativeArrayType<?, ?>> outerTypes;
        private final boolean allowMissing;
        private final boolean allowNull;

        public ArrayOfArrayProcessor(List<WritableChunk<?>> out, boolean allowMissing, boolean allowNull) {
            this.out = Objects.requireNonNull(out);
            this.outerTypes = outputTypesImpl().map(NativeArrayType::arrayType).collect(Collectors.toList());
            this.allowMissing = allowMissing;
            this.allowNull = allowNull;
        }

        @Override
        public Context start(JsonParser parser) throws IOException {
            return new ArrayOfArrayProcessorContext();
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {
            if (!allowNull) {
                throw Parsing.mismatch(parser, Object.class);
            }
            for (WritableChunk<?> writableChunk : out) {
                writableChunk.asWritableObjectChunk().add(null);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            if (!allowMissing) {
                throw Parsing.mismatchMissing(parser, Object.class);
            }
            for (WritableChunk<?> writableChunk : out) {
                writableChunk.asWritableObjectChunk().add(null);
            }
        }

        final class ArrayOfArrayProcessorContext implements Context {

            private final List<WritableChunk<?>> innerChunks;

            private ValueProcessorArrayImpl innerProcessor;

            public ArrayOfArrayProcessorContext() {
                innerChunks = outputTypesImpl()
                        .map(ObjectProcessor::chunkType)
                        .map(chunkType -> chunkType.makeWritableChunk(0))
                        .collect(Collectors.toList());
            }

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                if (isPow2(index)) {
                    resize(index);
                }
                innerProcessor.processCurrentValue(parser);
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                if (isPow2(index)) {
                    resize(index);
                }
                innerProcessor.processMissing(parser);
            }

            @Override
            public void done(JsonParser parser, int length) throws IOException {
                final int size = out.size();
                for (int i = 0; i < size; ++i) {
                    final WritableChunk<?> innerChunk = innerChunks.get(i);
                    final Object nativeArray = copy(innerChunk, outerTypes.get(i).componentType().clazz(), length);
                    innerChunk.close();
                    out.get(i).asWritableObjectChunk().add(nativeArray);
                }
            }

            private void resize(int index) {
                final int size = out.size();
                for (int i = 0; i < size; i++) {
                    final WritableChunk<?> innerChunk = innerChunks.get(i);
                    final WritableChunk<?> resized = resizeCopy(innerChunk, index, Math.max(index * 2, index + 1));
                    innerChunk.close();
                    innerChunks.set(i, resized);
                }
                innerProcessor = innerProcessor(Collections.unmodifiableList(innerChunks));
            }
        }
    }

    private static <ATTR extends Any> WritableChunk<ATTR> resizeCopy(WritableChunk<ATTR> in, int inSize,
            int outCapacity) {
        final WritableChunk<ATTR> out = in.getChunkType().makeWritableChunk(outCapacity);
        out.copyFromChunk(in, 0, 0, inSize);
        out.setSize(inSize);
        return out;
    }

    private static Object copy(WritableChunk<?> innerChunk, Class<?> componentClazz, int length) {
        final Object dest = Array.newInstance(componentClazz, length);
        innerChunk.copyToArray(0, dest, 0, length);
        return dest;
    }

    private static boolean isPow2(int x) {
        // true for 0, 1, 2, 4, 8, 16, ...
        return (x & (x - 1)) == 0;
    }
}
