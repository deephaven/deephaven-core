package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.util.hashing.*;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.util.SafeCloseable;

public interface ToDoubleCast extends SafeCloseable {
    <T> DoubleChunk<? extends T> cast(Chunk<? extends T> input);

    /**
     * Create a kernel that casts the values in an input chunk to an double.
     *
     * @param type the type of chunk, must be an integral primitive type
     * @param size the size of the largest chunk that can be cast by this kernel
     *
     * @return a {@link ToIntFunctor} that can be applied to chunks of type in order to produce a DoubleChunk of values
     */
    static ToDoubleCast makeToDoubleCast(ChunkType type, int size) {
        switch (type) {
            case Byte:
                return new ByteToDoubleCast(size);
            case Char:
                return new CharToDoubleCast(size);
            case Short:
                return new ShortToDoubleCast(size);
            case Int:
                return new IntToDoubleCast(size);
            case Long:
                return new LongToDoubleCast(size);
            case Float:
                return new FloatToDoubleCast(size);
            case Double:
                return IDENTITY;

            case Boolean:
            case Object:
        }
        throw new UnsupportedOperationException("Can not make toDoubleCast for " + type);
    }

    class Identity implements ToDoubleCast {
        @Override
        public <T> DoubleChunk<? extends T> cast(Chunk<? extends T> input) {
            return input.asDoubleChunk();
        }

        @Override
        public void close() {
        }
    }

    ToDoubleCast IDENTITY = new Identity();
}
