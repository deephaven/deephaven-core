package io.deephaven.db.v2.select.python;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Chunk.Visitor;
import io.deephaven.util.PrimitiveArrayType;

import java.util.Objects;
import java.util.stream.Stream;

class ArgumentsSingular {

    static Class<?>[] buildParamTypes(Chunk<?>[] __sources) {
        return Stream.of(__sources)
            .map(c -> c.walk(new ChunkToSingularType<>()))
            .map(ChunkToSingularType::getOut)
            .toArray(Class<?>[]::new);
    }

    static Object[] buildArguments(Chunk<?>[] __sources, int index) {
        return Stream.of(__sources)
            .map(c -> c.walk(new ChunkIndexToObject<>(index)))
            .map(ChunkIndexToObject::getOut)
            .toArray();
    }

    private static class ChunkIndexToObject<ATTR extends Any> implements Visitor<ATTR> {

        private final int index;

        private Object out;

        ChunkIndexToObject(int index) {
            this.index = index;
        }

        Object getOut() {
            return out;
        }

        @Override
        public void visit(ByteChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(BooleanChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(CharChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(ShortChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(IntChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(LongChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(FloatChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public void visit(DoubleChunk<ATTR> chunk) {
            out = chunk.get(index);
        }

        @Override
        public <T> void visit(ObjectChunk<T, ATTR> chunk) {
            out = chunk.get(index);
        }
    }

    private static class ChunkToSingularType<ATTR extends Any> implements Visitor<ATTR> {
        private Class<?> out;

        Class<?> getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ByteChunk<ATTR> chunk) {
            out = PrimitiveArrayType.bytes().getPrimitiveType();
        }

        @Override
        public void visit(BooleanChunk<ATTR> chunk) {
            out = PrimitiveArrayType.booleans().getPrimitiveType();
        }

        @Override
        public void visit(CharChunk<ATTR> chunk) {
            out = PrimitiveArrayType.chars().getPrimitiveType();
        }

        @Override
        public void visit(ShortChunk<ATTR> chunk) {
            out = PrimitiveArrayType.shorts().getPrimitiveType();
        }

        @Override
        public void visit(IntChunk<ATTR> chunk) {
            out = PrimitiveArrayType.ints().getPrimitiveType();
        }

        @Override
        public void visit(LongChunk<ATTR> chunk) {
            out = PrimitiveArrayType.longs().getPrimitiveType();
        }

        @Override
        public void visit(FloatChunk<ATTR> chunk) {
            out = PrimitiveArrayType.floats().getPrimitiveType();
        }

        @Override
        public void visit(DoubleChunk<ATTR> chunk) {
            out = PrimitiveArrayType.doubles().getPrimitiveType();
        }

        @Override
        public <T> void visit(ObjectChunk<T, ATTR> chunk) {
            // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to
            // return
            // the item type
            out = Object.class;
        }
    }
}
