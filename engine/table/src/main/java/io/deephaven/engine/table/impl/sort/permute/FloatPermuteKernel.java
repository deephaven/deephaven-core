/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class FloatPermuteKernel {
    public static <T extends Any> void permute(FloatChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableFloatChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static <T extends Any> void permuteInput(FloatChunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableFloatChunk<? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, FloatChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableFloatChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class FloatPermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            FloatPermuteKernel.permute(inputValues.asFloatChunk(), outputPositions, outputValues.asWritableFloatChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            FloatPermuteKernel.permute(inputPositions, inputValues.asFloatChunk(), outputPositions, outputValues.asWritableFloatChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            FloatPermuteKernel.permuteInput(inputValues.asFloatChunk(), inputPositions, outputValues.asWritableFloatChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new FloatPermuteKernelContext();
}
