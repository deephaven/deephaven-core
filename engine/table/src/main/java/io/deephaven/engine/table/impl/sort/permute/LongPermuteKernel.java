/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class LongPermuteKernel {
    public static <T extends Any> void permute(LongChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableLongChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static <T extends Any> void permuteInput(LongChunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableLongChunk<? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, LongChunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableLongChunk<? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class LongPermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            LongPermuteKernel.permute(inputValues.asLongChunk(), outputPositions, outputValues.asWritableLongChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            LongPermuteKernel.permute(inputPositions, inputValues.asLongChunk(), outputPositions, outputValues.asWritableLongChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            LongPermuteKernel.permuteInput(inputValues.asLongChunk(), inputPositions, outputValues.asWritableLongChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new LongPermuteKernelContext();
}
