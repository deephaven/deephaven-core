/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPermuteKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sort.permute;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.WritableFloatChunk;
import io.deephaven.engine.chunk.WritableIntChunk;
import junit.framework.TestCase;
import org.junit.Test;

public class TestFloatPermuteKernel {
    @Test
    public void testReverse() {
        final WritableFloatChunk<Attributes.Any> inputValues = WritableFloatChunk.makeWritableChunk(10);
        final WritableIntChunk<Attributes.ChunkPositions> destinations = WritableIntChunk.makeWritableChunk(10);
        final WritableFloatChunk<Attributes.Any> outputValues = WritableFloatChunk.makeWritableChunk(10);

        inputValues.setSize(0);
        for (float value = startingValue(); inputValues.size() < 10; ++value) {
            inputValues.add(value);
        }

        destinations.setSize(0);
        for (int ii = 9; ii >= 0; ii--) {
            destinations.add(ii);
        }

        FloatPermuteKernel.permute(inputValues, destinations, outputValues);

        float value = (float)(startingValue() + 9);
        for (int ii = 0; ii < 10; ++ii) {
            TestCase.assertEquals(value, outputValues.get(ii));
            value--;
        }
    }

    @Test
    public void testInterleave() {
        final WritableFloatChunk<Attributes.Any> inputValues = WritableFloatChunk.makeWritableChunk(10);
        final WritableIntChunk<Attributes.ChunkPositions> destinations = WritableIntChunk.makeWritableChunk(10);
        final WritableFloatChunk<Attributes.Any> outputValues = WritableFloatChunk.makeWritableChunk(10);

        inputValues.setSize(0);
        for (float value = startingValue(); inputValues.size() < 10; ++value) {
            inputValues.add(value);
        }

        destinations.setSize(0);
        final int half = inputValues.size() / 2;
        for (int ii = 0; ii < inputValues.size(); ii++) {
            if (ii % 2 == 0) {
                destinations.add(ii / 2);
            } else {
                destinations.add(half + ii / 2);
            }
        }

        FloatPermuteKernel.permute(inputValues, destinations, outputValues);

        for (int ii = 0; ii < inputValues.size(); ++ii) {
            if (ii < half) {
                TestCase.assertEquals(startingValue() + ii * 2, outputValues.get(ii));
            } else {
                TestCase.assertEquals(startingValue() + (ii - half) * 2 + 1, outputValues.get(ii));
            }
        }
    }

    @Test
    public void testSpread() {
        final WritableFloatChunk<Attributes.Any> inputValues = WritableFloatChunk.makeWritableChunk(10);
        final WritableIntChunk<Attributes.ChunkPositions> destinations = WritableIntChunk.makeWritableChunk(10);
        final WritableFloatChunk<Attributes.Any> outputValues = WritableFloatChunk.makeWritableChunk(20);
        outputValues.fillWithValue(0, outputValues.size(), uninitializedValue());

        inputValues.setSize(0);
        for (float value = startingValue(); inputValues.size() < 10; ++value) {
            inputValues.add(value);
        }

        destinations.setSize(0);
        final int half = inputValues.size() / 2;
        for (int ii = 0; ii < inputValues.size(); ii++) {
            if (ii % 2 == 0) {
                destinations.add(ii);
            } else {
                destinations.add(inputValues.size() + ii - 1);
            }
        }

        FloatPermuteKernel.permute(inputValues, destinations, outputValues);

        for (int ii = 0; ii < outputValues.size(); ++ii) {
            if (ii % 2 == 1) {
                TestCase.assertEquals(uninitializedValue(), outputValues.get(ii));
            }
            else if (ii / 2 < half) {
                TestCase.assertEquals(startingValue() + ii, outputValues.get(ii));
            } else {
                TestCase.assertEquals(startingValue() + (ii / 2 - half) * 2 + 1, outputValues.get(ii));
            }
        }
    }

    // region startValue
    private float startingValue() {
        return 1; // it can be convenient to change this to '0' or something else recognizable for float
    }
    // endregion startValue

    // region defaultValue
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    private static final float [] uninitializedValue = new float[1];
    private float uninitializedValue() {
        return uninitializedValue[0];
    }
    // endregion defaultValue
}
