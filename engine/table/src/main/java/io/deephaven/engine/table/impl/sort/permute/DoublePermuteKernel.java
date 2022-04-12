/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class DoublePermuteKernel {
    public static <T extends Any> void permute(DoubleChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableDoubleChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static <T extends Any> void permuteInput(DoubleChunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableDoubleChunk<? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, DoubleChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableDoubleChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class DoublePermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            DoublePermuteKernel.permute(inputValues.asDoubleChunk(), outputPositions, outputValues.asWritableDoubleChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            DoublePermuteKernel.permute(inputPositions, inputValues.asDoubleChunk(), outputPositions, outputValues.asWritableDoubleChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            DoublePermuteKernel.permuteInput(inputValues.asDoubleChunk(), inputPositions, outputValues.asWritableDoubleChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new DoublePermuteKernelContext();
}
