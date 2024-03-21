//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectProcessorNoopTest {

    @Test
    public void testNoopNoFill() {
        ObjectProcessor<Object> noop = ObjectProcessor.strict(ObjectProcessor.noop(List.of(
                Type.booleanType(),
                Type.charType(),
                Type.byteType(),
                Type.shortType(),
                Type.intType(),
                Type.longType(),
                Type.floatType(),
                Type.doubleType(),
                Type.stringType()), false));
        try (
                WritableObjectChunk<Object, ?> in = WritableObjectChunk.makeWritableChunk(1);
                WritableByteChunk<?> c1 = WritableByteChunk.makeWritableChunk(1);
                WritableCharChunk<?> c2 = WritableCharChunk.makeWritableChunk(1);
                WritableByteChunk<?> c3 = WritableByteChunk.makeWritableChunk(1);
                WritableShortChunk<?> c4 = WritableShortChunk.makeWritableChunk(1);
                WritableIntChunk<?> c5 = WritableIntChunk.makeWritableChunk(1);
                WritableLongChunk<?> c6 = WritableLongChunk.makeWritableChunk(1);
                WritableFloatChunk<?> c7 = WritableFloatChunk.makeWritableChunk(1);
                WritableDoubleChunk<?> c8 = WritableDoubleChunk.makeWritableChunk(1);
                WritableObjectChunk<String, ?> c9 = WritableObjectChunk.makeWritableChunk(1)) {
            List<WritableChunk<?>> out = List.of(c1, c2, c3, c4, c5, c6, c7, c8, c9);
            for (WritableChunk<?> c : out) {
                c.setSize(0);
            }
            in.set(0, new Object());
            c1.set(0, (byte) 42);
            c2.set(0, (char) 42);
            c3.set(0, (byte) 42);
            c4.set(0, (short) 42);
            c5.set(0, 42);
            c6.set(0, 42L);
            c7.set(0, 42.0f);
            c8.set(0, 42.0);
            c9.set(0, "42");
            noop.processAll(in, out);
            for (WritableChunk<?> c : out) {
                assertThat(c.size()).isEqualTo(1);
            }
            assertThat(c1.get(0)).isEqualTo((byte) 42);
            assertThat(c2.get(0)).isEqualTo((char) 42);
            assertThat(c3.get(0)).isEqualTo((byte) 42);
            assertThat(c4.get(0)).isEqualTo((short) 42);
            assertThat(c5.get(0)).isEqualTo(42);
            assertThat(c6.get(0)).isEqualTo(42L);
            assertThat(c7.get(0)).isEqualTo(42.0f);
            assertThat(c8.get(0)).isEqualTo(42.0);
            assertThat(c9.get(0)).isEqualTo("42");
        }
    }

    @Test
    public void testNoopNullFill() {
        ObjectProcessor<Object> noop = ObjectProcessor.strict(ObjectProcessor.noop(List.of(
                Type.booleanType(),
                Type.charType(),
                Type.byteType(),
                Type.shortType(),
                Type.intType(),
                Type.longType(),
                Type.floatType(),
                Type.doubleType(),
                Type.stringType()), true));
        try (
                WritableObjectChunk<Object, ?> in = WritableObjectChunk.makeWritableChunk(1);
                WritableByteChunk<?> c1 = WritableByteChunk.makeWritableChunk(1);
                WritableCharChunk<?> c2 = WritableCharChunk.makeWritableChunk(1);
                WritableByteChunk<?> c3 = WritableByteChunk.makeWritableChunk(1);
                WritableShortChunk<?> c4 = WritableShortChunk.makeWritableChunk(1);
                WritableIntChunk<?> c5 = WritableIntChunk.makeWritableChunk(1);
                WritableLongChunk<?> c6 = WritableLongChunk.makeWritableChunk(1);
                WritableFloatChunk<?> c7 = WritableFloatChunk.makeWritableChunk(1);
                WritableDoubleChunk<?> c8 = WritableDoubleChunk.makeWritableChunk(1);
                WritableObjectChunk<String, ?> c9 = WritableObjectChunk.makeWritableChunk(1)) {
            List<WritableChunk<?>> out = List.of(c1, c2, c3, c4, c5, c6, c7, c8, c9);
            for (WritableChunk<?> c : out) {
                c.setSize(0);
            }
            in.set(0, new Object());
            c1.set(0, (byte) 42);
            c2.set(0, (char) 42);
            c3.set(0, (byte) 42);
            c4.set(0, (short) 42);
            c5.set(0, 42);
            c6.set(0, 42L);
            c7.set(0, 42.0f);
            c8.set(0, 42.0);
            c9.set(0, "42");
            noop.processAll(in, out);
            for (WritableChunk<?> c : out) {
                assertThat(c.size()).isEqualTo(1);
            }
            assertThat(c1.get(0)).isEqualTo(QueryConstants.NULL_BYTE);
            assertThat(c2.get(0)).isEqualTo(QueryConstants.NULL_CHAR);
            assertThat(c3.get(0)).isEqualTo(QueryConstants.NULL_BYTE);
            assertThat(c4.get(0)).isEqualTo(QueryConstants.NULL_SHORT);
            assertThat(c5.get(0)).isEqualTo(QueryConstants.NULL_INT);
            assertThat(c6.get(0)).isEqualTo(QueryConstants.NULL_LONG);
            assertThat(c7.get(0)).isEqualTo(QueryConstants.NULL_FLOAT);
            assertThat(c8.get(0)).isEqualTo(QueryConstants.NULL_DOUBLE);
            assertThat(c9.get(0)).isNull();
        }
    }
}
