/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class IntPermuteKernel {
    public static <T extends Any> void permute(IntChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableIntChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static <T extends Any> void permuteInput(IntChunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableIntChunk<? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, IntChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableIntChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class IntPermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            IntPermuteKernel.permute(inputValues.asIntChunk(), outputPositions, outputValues.asWritableIntChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            IntPermuteKernel.permute(inputPositions, inputValues.asIntChunk(), outputPositions, outputValues.asWritableIntChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            IntPermuteKernel.permuteInput(inputValues.asIntChunk(), inputPositions, outputValues.asWritableIntChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new IntPermuteKernelContext();
}
