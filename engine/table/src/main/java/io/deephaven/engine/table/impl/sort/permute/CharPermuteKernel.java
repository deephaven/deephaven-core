package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class CharPermuteKernel {
    public static <T extends Any> void permute(CharChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableCharChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static <T extends Any> void permuteInput(CharChunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableCharChunk<? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, CharChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableCharChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class CharPermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            CharPermuteKernel.permute(inputValues.asCharChunk(), outputPositions, outputValues.asWritableCharChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            CharPermuteKernel.permute(inputPositions, inputValues.asCharChunk(), outputPositions, outputValues.asWritableCharChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            CharPermuteKernel.permuteInput(inputValues.asCharChunk(), inputPositions, outputValues.asWritableCharChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new CharPermuteKernelContext();
}
