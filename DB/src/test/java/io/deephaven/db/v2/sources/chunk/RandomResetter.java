package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.ArrayGenerator;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import java.util.Random;

public class RandomResetter {
    static RandomResetter makeRandomResetter(ChunkType chunkType) {
        switch (chunkType) {
            case Boolean:
                return new RandomResetter(ArrayGenerator::randomBoxedBooleans);
            case Byte:
                return new RandomResetter(ArrayGenerator::randomBytes);
            case Char:
                return new RandomResetter(ArrayGenerator::randomChars);
            case Double:
                return new RandomResetter(ArrayGenerator::randomDoubles);
            case Float:
                return new RandomResetter(ArrayGenerator::randomFloats);
            case Int:
                return new RandomResetter(ArrayGenerator::randomInts);
            case Long:
                return new RandomResetter(ArrayGenerator::randomLongs);
            case Short:
                return new RandomResetter(ArrayGenerator::randomShorts);
            case Object:
                return new RandomResetter(ArrayGenerator::randomObjects);
            default:
                throw new UnsupportedOperationException(
                    "Can't make RandomResetter for " + chunkType);
        }
    }

    private final GeneratorInvoker invoker;

    private RandomResetter(GeneratorInvoker invoker) {
        this.invoker = invoker;
    }

    void resetWithRandomValues(Random rng, ResettableWritableChunk<Values> dest, int size) {
        final Object array = invoker.invoke(rng, size);
        dest.resetFromArray(array, 0, size);
    }

    private interface GeneratorInvoker {
        Object invoke(Random rng, int size);
    }
}
