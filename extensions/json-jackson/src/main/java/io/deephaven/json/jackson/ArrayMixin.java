//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.json.ArrayValue;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ArrayMixin extends Mixin<ArrayValue> {

    public ArrayMixin(ArrayValue options, JsonFactory factory) {
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
    public ValueProcessor processor(String context) {
        return innerProcessor();
    }

    Stream<? extends Type<?>> elementOutputTypes() {
        return element().outputTypesImpl();
    }

    RepeaterProcessor elementRepeater() {
        return element().repeaterProcessor(allowMissing(), allowNull());
    }

    private ArrayValueProcessor innerProcessor() {
        return new ArrayValueProcessor(elementRepeater());
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        // For example:
        // double (element())
        // double[] (processor())
        // double[][] (arrayProcessor())
        return new ArrayOfArrayProcessor(allowMissing, allowNull);
    }

    final class ArrayOfArrayProcessor implements RepeaterProcessor {
        private final List<NativeArrayType<?, ?>> outerTypes;
        private final boolean allowMissing;
        private final boolean allowNull;

        private List<WritableChunk<?>> out;

        public ArrayOfArrayProcessor(boolean allowMissing, boolean allowNull) {
            this.outerTypes = outputTypesImpl().map(NativeArrayType::arrayType).collect(Collectors.toList());
            this.allowMissing = allowMissing;
            this.allowNull = allowNull;
        }

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            this.out = out;
        }

        @Override
        public void clearContext() {
            out = null;
        }

        @Override
        public int numColumns() {
            return ArrayMixin.this.numColumns();
        }

        @Override
        public Context context() {
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

            private ArrayValueProcessor innerProcessor;

            public ArrayOfArrayProcessorContext() {
                innerChunks = outputTypesImpl()
                        .map(ObjectProcessor::chunkType)
                        .map(chunkType -> chunkType.makeWritableChunk(0))
                        .collect(Collectors.toList());
            }

            @Override
            public void start(JsonParser parser) throws IOException {

            }

            @Override
            public void processElement(JsonParser parser) throws IOException {
                if (isPow2(index)) {
                    resize(index);
                }
                innerProcessor.processCurrentValue(parser);
            }

            @Override
            public void processElementMissing(JsonParser parser) throws IOException {
                if (isPow2(index)) {
                    resize(index);
                }
                innerProcessor.processMissing(parser);
            }

            @Override
            public void done(JsonParser parser) throws IOException {
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
                innerProcessor = innerProcessor();
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
