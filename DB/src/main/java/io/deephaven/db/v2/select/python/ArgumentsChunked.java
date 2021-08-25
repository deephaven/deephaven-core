package io.deephaven.db.v2.select.python;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.PrimitiveArrayType;

import java.util.Objects;

class ArgumentsChunked {
    static ArgumentsChunked buildArguments(Chunk<?>[] __sources) {
        final Class<?>[] paramTypes = new Class[__sources.length];
        final Object[] params = new Object[__sources.length];
        for (int i = 0; i < __sources.length; i++) {
            final ChunkToArray<?> cta = __sources[i].walk(new ChunkToArray<>());
            paramTypes[i] = Objects.requireNonNull(cta.getArrayType());
            params[i] = Objects.requireNonNull(cta.getArray());
        }
        return new ArgumentsChunked(paramTypes, params);
    }

    private final Class<?>[] paramTypes;
    private final Object[] params;

    private ArgumentsChunked(Class<?>[] paramTypes, Object[] params) {
        this.paramTypes = paramTypes;
        this.params = params;
    }

    Class<?>[] getParamTypes() {
        return paramTypes;
    }

    Object[] getParams() {
        return params;
    }

    private static class ChunkToArray<ATTR extends Any> implements Chunk.Visitor<ATTR> {

        private Class<?> arrayType;
        private Object array;

        Class<?> getArrayType() {
            return Objects.requireNonNull(arrayType);
        }

        Object getArray() {
            return Objects.requireNonNull(array);
        }

        @Override
        public void visit(ByteChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.bytes().getArrayType();
            final byte[] out = PrimitiveArrayType.bytes().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(BooleanChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.booleans().getArrayType();
            final boolean[] out = PrimitiveArrayType.booleans().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(CharChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.chars().getArrayType();
            final char[] out = PrimitiveArrayType.chars().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(ShortChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.shorts().getArrayType();
            final short[] out = PrimitiveArrayType.shorts().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(IntChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.ints().getArrayType();
            final int[] out = PrimitiveArrayType.ints().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(LongChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.longs().getArrayType();
            final long[] out = PrimitiveArrayType.longs().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(FloatChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.floats().getArrayType();
            final float[] out = PrimitiveArrayType.floats().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public void visit(DoubleChunk<ATTR> chunk) {
            arrayType = PrimitiveArrayType.doubles().getArrayType();
            final double[] out = PrimitiveArrayType.doubles().newInstance(chunk.size());
            chunk.copyToTypedArray(0, out, 0, out.length);
            array = out;
        }

        @Override
        public <T> void visit(ObjectChunk<T, ATTR> chunk) {
            // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to return
            // the array type
            arrayType = Object[].class;
            final Object[] out = new Object[chunk.size()];
            chunk.copyToArray(0, out, 0, out.length);
            array = out;
        }
    }
}
