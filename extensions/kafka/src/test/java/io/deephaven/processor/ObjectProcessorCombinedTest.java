/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
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
import org.junit.Test;

import java.util.List;

import static io.deephaven.processor.ObjectProcessor.combined;
import static io.deephaven.processor.ObjectProcessor.noop;
import static io.deephaven.processor.ObjectProcessor.strict;
import static org.assertj.core.api.Assertions.assertThat;

public class ObjectProcessorCombinedTest {

    @Test
    public void testCombined() {
        final ObjectProcessor<Object> p1 = strict(noop(List.of(Type.intType(), Type.longType()), true));
        final ObjectProcessor<Object> p2 = strict(noop(List.of(Type.doubleType()), true));
        final ObjectProcessor<Object> combined = strict(combined(List.of(p1, p2)));;
        assertThat(combined.outputTypes()).containsExactly(Type.intType(), Type.longType(), Type.doubleType());
        try (
                WritableObjectChunk<Object, ?> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<?> c1 = WritableIntChunk.makeWritableChunk(1);
                WritableLongChunk<?> c2 = WritableLongChunk.makeWritableChunk(1);
                WritableDoubleChunk<?> c3 = WritableDoubleChunk.makeWritableChunk(1)) {
            List<WritableChunk<?>> out = List.of(c1, c2, c3);
            for (WritableChunk<?> c : out) {
                c.setSize(0);
            }
            in.set(0, new Object());
            c1.set(0, 42);
            c2.set(0, 42L);
            c3.set(0, 42.0);
            combined.processAll(in, out);
            for (WritableChunk<?> c : out) {
                assertThat(c.size()).isEqualTo(1);
            }
            assertThat(c1.get(0)).isEqualTo(QueryConstants.NULL_INT);
            assertThat(c2.get(0)).isEqualTo(QueryConstants.NULL_LONG);
            assertThat(c3.get(0)).isEqualTo(QueryConstants.NULL_DOUBLE);
        }
    }
}
