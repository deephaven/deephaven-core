package io.deephaven.db.v2.ssa;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TstUtils;
import org.jetbrains.annotations.NotNull;

public class SsaTestHelpers {
    @NotNull
    public static TstUtils.SortedIntGenerator getGeneratorForChar() {
        return new TstUtils.SortedIntGenerator((int) Character.MIN_VALUE + 1, (int) Character.MAX_VALUE - 1);
    }

    public static Table prepareTestTableForChar(QueryTable table) {
        return table.updateView("Value=(char)Value");
    }

    @NotNull
    public static TstUtils.SortedIntGenerator getGeneratorForByte() {
        return new TstUtils.SortedIntGenerator(Byte.MIN_VALUE + 1, Byte.MAX_VALUE - 1);
    }

    public static Table prepareTestTableForByte(QueryTable table) {
        return table.updateView("Value=(byte)Value");
    }

    @NotNull
    public static TstUtils.SortedIntGenerator getGeneratorForShort() {
        return new TstUtils.SortedIntGenerator(Short.MIN_VALUE + 1, (int) Short.MAX_VALUE);
    }

    public static Table prepareTestTableForShort(QueryTable table) {
        return table.updateView("Value=(short)Value");
    }

    @NotNull
    public static TstUtils.SortedIntGenerator getGeneratorForInt() {
        return new TstUtils.SortedIntGenerator(-100000, 100000);
    }

    public static Table prepareTestTableForInt(QueryTable table) {
        return table;
    }

    @NotNull
    public static TstUtils.SortedLongGenerator getGeneratorForLong() {
        return new TstUtils.SortedLongGenerator(2L * Integer.MIN_VALUE, 2L * Integer.MAX_VALUE);
    }

    public static Table prepareTestTableForLong(QueryTable table) {
        return table;
    }

    @NotNull
    public static TstUtils.SortedDoubleGenerator getGeneratorForFloat() {
        return new TstUtils.SortedDoubleGenerator(-10000.0, 10000.0);
    }

    public static Table prepareTestTableForFloat(QueryTable table) {
        return table.updateView("Value=(float)Value");
    }

    @NotNull
    public static TstUtils.SortedDoubleGenerator getGeneratorForDouble() {
        return new TstUtils.SortedDoubleGenerator(-10000.0, 10000.0);
    }

    public static Table prepareTestTableForDouble(QueryTable table) {
        return table;
    }

    @NotNull
    public static TstUtils.SortedIntGenerator getGeneratorForObject() {
        return new TstUtils.SortedIntGenerator(0, 100000);
    }

    public static Table prepareTestTableForObject(QueryTable table) {
        // an update might be faster, but updateView ensures we break when object equality is not the same as ==
        return LiveTableMonitor.DEFAULT.sharedLock()
                .computeLocked(() -> table.updateView("Value=String.format(`%06d`, Value)"));
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
