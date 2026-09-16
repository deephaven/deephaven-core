//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.generator.SortedDoubleGenerator;
import io.deephaven.engine.testutil.generator.SortedIntGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;

public class SsaTestHelpers {
    @NotNull
    public static SortedIntGenerator getGeneratorForChar() {
        return new SortedIntGenerator((int) Character.MIN_VALUE, (int) Character.MAX_VALUE - 1);
    }

    public static Table prepareTestTableForChar(QueryTable table) {
        return table.updateView("Value=(char)Value");
    }

    @NotNull
    public static SortedIntGenerator getGeneratorForByte() {
        return new SortedIntGenerator(Byte.MIN_VALUE + 1, Byte.MAX_VALUE - 1);
    }

    public static Table prepareTestTableForByte(QueryTable table) {
        return table.updateView("Value=(byte)Value");
    }

    @NotNull
    public static SortedIntGenerator getGeneratorForShort() {
        return new SortedIntGenerator(Short.MIN_VALUE + 1, (int) Short.MAX_VALUE);
    }

    public static Table prepareTestTableForShort(QueryTable table) {
        return table.updateView("Value=(short)Value");
    }

    @NotNull
    public static SortedIntGenerator getGeneratorForInt() {
        return new SortedIntGenerator(-100000, 100000);
    }

    public static Table prepareTestTableForInt(QueryTable table) {
        return table;
    }

    @NotNull
    public static SortedLongGenerator getGeneratorForLong() {
        return new SortedLongGenerator(2L * Integer.MIN_VALUE, 2L * Integer.MAX_VALUE);
    }

    public static Table prepareTestTableForLong(QueryTable table) {
        return table;
    }

    @NotNull
    public static SortedDoubleGenerator getGeneratorForFloat() {
        return new SortedDoubleGenerator(-10000.0, 10000.0);
    }

    public static Table prepareTestTableForFloat(QueryTable table) {
        return table.updateView("Value=(float)Value");
    }

    @NotNull
    public static SortedDoubleGenerator getGeneratorForDouble() {
        return new SortedDoubleGenerator(-10000.0, 10000.0);
    }

    public static Table prepareTestTableForDouble(QueryTable table) {
        return table;
    }

    @NotNull
    public static SortedIntGenerator getGeneratorForObject() {
        return new SortedIntGenerator(0, 100000);
    }

    /**
     * Zero-pads {@code value} to a width of six, matching {@code String.format("%06d", value)} over the non-negative
     * range produced by {@link #getGeneratorForObject()}, so that the lexicographic ordering of the results matches the
     * numeric ordering of the inputs.
     *
     * <p>
     * Every call allocates a new String, because the tests rely on distinct instances to catch code that compares
     * values with {@code ==} rather than {@code equals}. The method holds no state, so it is safe to evaluate from
     * several select threads at once.
     */
    public static String formatObjectValue(final int value) {
        final String digits = Integer.toString(value);
        final int width = Math.max(6, digits.length());
        final char[] chars = new char[width];
        final int offset = width - digits.length();
        for (int ii = 0; ii < offset; ++ii) {
            chars[ii] = '0';
        }
        digits.getChars(0, digits.length(), chars, offset);
        return new String(chars);
    }

    public static Table prepareTestTableForObject(QueryTable table) {
        // an update might be faster, but updateView ensures we break when object equality is not the same as ==
        return ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> table.updateView(
                        "Value=io.deephaven.engine.table.impl.ssa.SsaTestHelpers.formatObjectValue(Value)"));
    }

    public static final class TestDescriptor {

        private int seed;
        private int tableSize;
        private int nodeSize;
        private int step;

        public TestDescriptor reset(final int seed, final int tableSize, final int nodeSize) {
            this.seed = seed;
            this.tableSize = tableSize;
            this.nodeSize = nodeSize;
            step = -1;
            return this;
        }

        public boolean advance(final int maxSteps) {
            return ++step < maxSteps;
        }

        public void advance() {
            ++step;
        }

        @Override
        public String toString() {
            return "seed = " + seed + ", tableSize=" + tableSize + ", nodeSize=" + nodeSize + ", step = " + step;
        }

        public int seed() {
            return seed;
        }

        public int tableSize() {
            return tableSize;
        }

        public int nodeSize() {
            return nodeSize;
        }
    }
}
