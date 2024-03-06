//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.function;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.function.ToByteFunction;
import io.deephaven.function.ToCharFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToShortFunction;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.util.BooleanUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectProcessorFunctionsTest {

    @Test
    public void testObjectProcessorFunctionsCorrectAndColumnOriented() {
        int numRows = 128;
        FunctionOrchestrator orchestrator = new FunctionOrchestrator(numRows);
        ObjectProcessor<Object> functions = ObjectProcessor.strict(ObjectProcessorFunctions.of(List.of(
                ToObjectFunction.of(orchestrator::toBoolean, Type.booleanType().boxedType()),
                (ToCharFunction<Object>) orchestrator::toChar,
                (ToByteFunction<Object>) orchestrator::toByte,
                (ToShortFunction<Object>) orchestrator::toShort,
                (ToIntFunction<Object>) orchestrator::toInt,
                (ToLongFunction<Object>) orchestrator::toLong,
                (ToFloatFunction<Object>) orchestrator::toFloat,
                (ToDoubleFunction<Object>) orchestrator::toDouble,
                ToObjectFunction.of(orchestrator::toString, Type.stringType()),
                ToObjectFunction.of(orchestrator::toInstant, Type.instantType()))));
        try (
                WritableObjectChunk<Object, ?> in = WritableObjectChunk.makeWritableChunk(numRows);
                WritableByteChunk<?> c1 = WritableByteChunk.makeWritableChunk(numRows);
                WritableCharChunk<?> c2 = WritableCharChunk.makeWritableChunk(numRows);
                WritableByteChunk<?> c3 = WritableByteChunk.makeWritableChunk(numRows);
                WritableShortChunk<?> c4 = WritableShortChunk.makeWritableChunk(numRows);
                WritableIntChunk<?> c5 = WritableIntChunk.makeWritableChunk(numRows);
                WritableLongChunk<?> c6 = WritableLongChunk.makeWritableChunk(numRows);
                WritableFloatChunk<?> c7 = WritableFloatChunk.makeWritableChunk(numRows);
                WritableDoubleChunk<?> c8 = WritableDoubleChunk.makeWritableChunk(numRows);
                WritableObjectChunk<String, ?> c9 = WritableObjectChunk.makeWritableChunk(numRows);
                WritableLongChunk<?> c10 = WritableLongChunk.makeWritableChunk(numRows)) {
            List<WritableChunk<?>> out = List.of(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10);
            for (WritableChunk<?> c : out) {
                c.setSize(0);
            }
            functions.processAll(in, out);
            for (WritableChunk<?> c : out) {
                assertThat(c.size()).isEqualTo(numRows);
            }
            for (int i = 0; i < numRows; ++i) {
                assertThat(c1.get(i)).isEqualTo(BooleanUtils.booleanAsByte(true));
                assertThat(c2.get(i)).isEqualTo((char) 42);
                assertThat(c3.get(i)).isEqualTo((byte) 42);
                assertThat(c4.get(i)).isEqualTo((short) 42);
                assertThat(c5.get(i)).isEqualTo(42);
                assertThat(c6.get(i)).isEqualTo(42L);
                assertThat(c7.get(i)).isEqualTo(42.0f);
                assertThat(c8.get(i)).isEqualTo(42.0);
                assertThat(c9.get(i)).isEqualTo("42");
                assertThat(c10.get(i)).isEqualTo(42000000L);
            }
        }
        orchestrator.assertDone();
    }

    private static class FunctionOrchestrator {

        private final int numRows;

        private int booleans;
        private int chars;
        private int bytes;
        private int shorts;
        private int ints;
        private int longs;
        private int floats;
        private int doubles;
        private int strings;
        private int instants;

        public FunctionOrchestrator(int numRows) {
            this.numRows = numRows;
        }

        Boolean toBoolean(Object ignore) {
            ++booleans;
            return true;
        }

        char toChar(Object ignore) {
            assertThat(booleans).isEqualTo(numRows);
            ++chars;
            return (char) 42;
        }

        byte toByte(Object ignore) {
            assertThat(chars).isEqualTo(numRows);
            ++bytes;
            return (byte) 42;
        }

        short toShort(Object ignore) {
            assertThat(bytes).isEqualTo(numRows);
            ++shorts;
            return (short) 42;
        }

        int toInt(Object ignore) {
            assertThat(shorts).isEqualTo(numRows);
            ++ints;
            return (short) 42;
        }

        long toLong(Object ignore) {
            assertThat(ints).isEqualTo(numRows);
            ++longs;
            return 42L;
        }

        float toFloat(Object ignore) {
            assertThat(longs).isEqualTo(numRows);
            ++floats;
            return 42.0f;
        }

        double toDouble(Object ignore) {
            assertThat(floats).isEqualTo(numRows);
            ++doubles;
            return 42.0;
        }

        String toString(Object ignore) {
            assertThat(doubles).isEqualTo(numRows);
            ++strings;
            return "42";
        }

        Instant toInstant(Object ignore) {
            assertThat(strings).isEqualTo(numRows);
            ++instants;
            return Instant.ofEpochMilli(42);
        }

        void assertDone() {
            assertThat(instants).isEqualTo(numRows);
        }
    }
}
