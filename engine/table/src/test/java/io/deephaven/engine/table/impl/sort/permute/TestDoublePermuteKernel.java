/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharPermuteKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.permute;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import junit.framework.TestCase;
import org.junit.Test;

public class TestDoublePermuteKernel {
    @Test
    public void testReverse() {
        final WritableDoubleChunk<Any> inputValues = WritableDoubleChunk.makeWritableChunk(10);
        final WritableIntChunk<ChunkPositions> destinations = WritableIntChunk.makeWritableChunk(10);
        final WritableDoubleChunk<Any> outputValues = WritableDoubleChunk.makeWritableChunk(10);

        inputValues.setSize(0);
        for (double value = startingValue(); inputValues.size() < 10; ++value) {
            inputValues.add(value);
        }

        destinations.setSize(0);
        for (int ii = 9; ii >= 0; ii--) {
            destinations.add(ii);
        }

        DoublePermuteKernel.permute(inputValues, destinations, outputValues);

        double value = (double)(startingValue() + 9);
        for (int ii = 0; ii < 10; ++ii) {
            TestCase.assertEquals(value, outputValues.get(ii));
            value--;
        }
    }

    @Test
    public void testInterleave() {
        final WritableDoubleChunk<Any> inputValues = WritableDoubleChunk.makeWritableChunk(10);
        final WritableIntChunk<ChunkPositions> destinations = WritableIntChunk.makeWritableChunk(10);
        final WritableDoubleChunk<Any> outputValues = WritableDoubleChunk.makeWritableChunk(10);

        inputValues.setSize(0);
        for (double value = startingValue(); inputValues.size() < 10; ++value) {
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

        DoublePermuteKernel.permute(inputValues, destinations, outputValues);

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
        final WritableDoubleChunk<Any> inputValues = WritableDoubleChunk.makeWritableChunk(10);
        final WritableIntChunk<ChunkPositions> destinations = WritableIntChunk.makeWritableChunk(10);
        final WritableDoubleChunk<Any> outputValues = WritableDoubleChunk.makeWritableChunk(20);
        outputValues.fillWithValue(0, outputValues.size(), uninitializedValue());

        inputValues.setSize(0);
        for (double value = startingValue(); inputValues.size() < 10; ++value) {
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

        DoublePermuteKernel.permute(inputValues, destinations, outputValues);

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
    private double startingValue() {
        return 1; // it can be convenient to change this to '0' or something else recognizable for double
    }
    // endregion startValue

    // region defaultValue
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    private static final double [] uninitializedValue = new double[1];
    private double uninitializedValue() {
        return uninitializedValue[0];
    }
    // endregion defaultValue
}
