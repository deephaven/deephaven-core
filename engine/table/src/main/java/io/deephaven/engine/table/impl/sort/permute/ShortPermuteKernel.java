/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class ShortPermuteKernel {
    public static <T extends Any> void permute(ShortChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableShortChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static <T extends Any> void permuteInput(ShortChunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableShortChunk<? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, ShortChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableShortChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class ShortPermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            ShortPermuteKernel.permute(inputValues.asShortChunk(), outputPositions, outputValues.asWritableShortChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            ShortPermuteKernel.permute(inputPositions, inputValues.asShortChunk(), outputPositions, outputValues.asWritableShortChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            ShortPermuteKernel.permuteInput(inputValues.asShortChunk(), inputPositions, outputValues.asWritableShortChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new ShortPermuteKernelContext();
}
