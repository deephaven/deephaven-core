/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.qst.type.Type;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectProcessorRowLimitedTest {

    @Test
    public void testRowLimit() {
        for (int rowLimit : new int[] {1, 2, 3, 4, 7, 8, 9, 16, 32, 64, 127, 128, 129, 256}) {
            for (int totalRows : new int[] {0, 1, 2, 3, 4, 7, 8, 9, 16, 32, 64, 127, 128, 129, 256}) {
                doRowLimitCheck(rowLimit, totalRows);
            }
        }
    }

    private static void doRowLimitCheck(int rowLimit, int totalRows) {
        RowLimitedCheckerImpl<Object> checker = new RowLimitedCheckerImpl<>(rowLimit, totalRows);
        ObjectProcessor<Object> rowLimited = ObjectProcessor.strict(ObjectProcessor.rowLimited(checker, rowLimit));
        try (
                WritableObjectChunk<Object, ?> in = WritableObjectChunk.makeWritableChunk(totalRows);
                WritableIntChunk<?> c1 = WritableIntChunk.makeWritableChunk(totalRows)) {
            c1.setSize(0);
            rowLimited.processAll(in, List.of(c1));
        }
        checker.assertDone();
    }

    private static class RowLimitedCheckerImpl<T> implements ObjectProcessor<T> {
        private static final ObjectProcessor<Object> NOOP = ObjectProcessor.noop(List.of(Type.intType()), false);
        private final int rowLimit;
        private final int totalRows;
        private int cumulativeInSize;

        public RowLimitedCheckerImpl(int rowLimit, int totalRows) {
            this.rowLimit = rowLimit;
            this.totalRows = totalRows;
        }

        @Override
        public List<Type<?>> outputTypes() {
            return NOOP.outputTypes();
        }

        @Override
        public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
            final int expectedInSize = Math.min(totalRows - cumulativeInSize, rowLimit);
            assertThat(in.size()).isEqualTo(expectedInSize);
            cumulativeInSize += expectedInSize;
            NOOP.processAll(in, out);
        }

        public void assertDone() {
            assertThat(cumulativeInSize).isEqualTo(totalRows);
        }
    }
}
