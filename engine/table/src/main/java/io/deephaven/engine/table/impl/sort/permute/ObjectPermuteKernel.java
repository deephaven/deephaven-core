/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public class ObjectPermuteKernel {
    public static<TYPE_T, T extends Any> void permute(ObjectChunk<TYPE_T, ? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableObjectChunk<TYPE_T, ? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(ii));
        }
    }

    public static<TYPE_T, T extends Any> void permuteInput(ObjectChunk<TYPE_T, ? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableObjectChunk<TYPE_T, ? super T> outputValues) {
        for (int ii = 0; ii < inputPositions.size(); ++ii) {
            outputValues.set(ii, inputValues.get(inputPositions.get(ii)));
        }
    }

    public static<TYPE_T, T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, ObjectChunk<TYPE_T, ? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableObjectChunk<TYPE_T, ? super T> outputValues) {
        for (int ii = 0; ii < outputPositions.size(); ++ii) {
            outputValues.set(outputPositions.get(ii), inputValues.get(inputPositions.get(ii)));
        }
    }

    private static class ObjectPermuteKernelContext implements PermuteKernel {
        @Override
        public <T extends Any> void permute(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            ObjectPermuteKernel.permute(inputValues.<Object>asObjectChunk(), outputPositions, outputValues.<Object>asWritableObjectChunk());
        }

        @Override
        public <T extends Any> void permute(IntChunk<ChunkPositions> inputPositions, Chunk<? extends T> inputValues, IntChunk<ChunkPositions> outputPositions, WritableChunk<? super T> outputValues) {
            ObjectPermuteKernel.permute(inputPositions, inputValues.<Object>asObjectChunk(), outputPositions, outputValues.<Object>asWritableObjectChunk());
        }

        @Override
        public <T extends Any> void permuteInput(Chunk<? extends T> inputValues, IntChunk<ChunkPositions> inputPositions, WritableChunk<? super T> outputValues) {
            ObjectPermuteKernel.permuteInput(inputValues.<Object>asObjectChunk(), inputPositions, outputValues.<Object>asWritableObjectChunk());
        }
    }

    public final static PermuteKernel INSTANCE = new ObjectPermuteKernelContext();
}
