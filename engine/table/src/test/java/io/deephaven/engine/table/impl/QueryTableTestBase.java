package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.util.TableTools;
import org.apache.commons.lang3.mutable.MutableInt;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * QueryTable tests can extend this to get convenient EvalNuggets, JoinIncrementors, etc.
 */
public abstract class QueryTableTestBase extends RefreshingTableTestCase {
    protected final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    private static final GenerateTableUpdates.SimulationProfile NO_SHIFT_PROFILE =
            new GenerateTableUpdates.SimulationProfile();
    static {
        NO_SHIFT_PROFILE.SHIFT_10_PERCENT_KEY_SPACE = 0;
        NO_SHIFT_PROFILE.SHIFT_10_PERCENT_POS_SPACE = 0;
        NO_SHIFT_PROFILE.SHIFT_AGGRESSIVELY = 0;
    }

    public final JoinIncrement leftStep = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            simulateShiftAwareStep(NO_SHIFT_PROFILE, toString(), leftSize, random, leftTable, leftColumnInfo, en);
        }

        @Override
        public String toString() {
            return "Left Step";
        }
    };
    public final JoinIncrement leftStepShift = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            simulateShiftAwareStep(toString(), leftSize, random, leftTable, leftColumnInfo, en);
        }

        @Override
        public String toString() {
            return "Left Shift Step";
        }
    };
    final JoinIncrement rightStep = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            simulateShiftAwareStep(NO_SHIFT_PROFILE, toString(), rightSize, random, rightTable, rightColumnInfo, en);
        }

        @Override
        public String toString() {
            return "Right Step";
        }
    };
    public final JoinIncrement rightStepShift = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            simulateShiftAwareStep(toString(), rightSize, random, rightTable, rightColumnInfo, en);
        }

        @Override
        public String toString() {
            return "Right Shift Step";
        }
    };
    public final JoinIncrement leftRightStep = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            simulateShiftAwareStep(NO_SHIFT_PROFILE, toString(), leftSize, random, leftTable, leftColumnInfo, en);
            simulateShiftAwareStep(NO_SHIFT_PROFILE, toString(), rightSize, random, rightTable, rightColumnInfo, en);
        }

        @Override
        public String toString() {
            return "Left and Right Step";
        }
    };
    public final JoinIncrement leftRightStepShift = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            simulateShiftAwareStep(toString(), leftSize, random, leftTable, leftColumnInfo, en);
            simulateShiftAwareStep(toString(), rightSize, random, rightTable, rightColumnInfo, en);
        }

        @Override
        public String toString() {
            return "Left and Right Shift Step";
        }
    };

    public final JoinIncrement leftRightConcurrentStepShift = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, leftSize,
                        random, leftTable, leftColumnInfo);
                GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, rightSize,
                        random, rightTable, rightColumnInfo);
            });
        }

        @Override
        public String toString() {
            return "Left and Right Concurrent Shift Step";
        }
    };

    public final JoinIncrement[] joinIncrementors = new JoinIncrement[] {leftStep, rightStep, leftRightStep};
    final JoinIncrement[] joinIncrementorsShift = new JoinIncrement[] {leftStep, rightStep, leftRightStep,
            leftStepShift, rightStepShift, leftRightStepShift, leftRightConcurrentStepShift};

    protected RowSet added;
    protected RowSet removed;
    protected RowSet modified;

    protected interface JoinIncrement {
        void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random);
    }

    public static class TableComparator implements EvalNuggetInterface {
        private Table t1, t2;
        private String t1Name, t2Name;

        public TableComparator(Table t1, Table t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        TableComparator(Table t1, String t1Name, Table t2, String t2Name) {
            this.t1 = t1;
            this.t2 = t2;
            this.t1Name = t1Name;
            this.t2Name = t2Name;
            validate("Initial construction.");
        }

        @Override
        public void validate(String msg) {
            TstUtils.assertTableEquals(msg, t2, t1, TableDiff.DiffItems.DoublesExact);
        }

        @Override
        public void show() {
            System.out.println(t1Name != null ? t1Name : t1);
            TableTools.showWithRowSet(t1);
            System.out.println(t2Name != null ? t2Name : t2);
            TableTools.showWithRowSet(t2);
        }
    }

    public static int[] intColumn(Table table, String column) {
        final int[] result = new int[table.intSize()];
        final MutableInt pos = new MutableInt();
        table.integerColumnIterator(column).forEachRemaining((int value) -> {
            result[pos.getValue()] = value;
            pos.increment();
        });
        return result;
    }

    protected static class CoalescingListener extends ShiftObliviousInstrumentedListenerAdapter {
        RowSet lastAdded, lastModified, lastRemoved;
        ShiftObliviousUpdateCoalescer indexUpdateCoalescer = new ShiftObliviousUpdateCoalescer();

        protected CoalescingListener(Table source) {
            super(source, false);
        }

        public int getCount() {
            return count;
        }

        int count;

        void reset() {
            count = 0;
            lastAdded = null;
            lastRemoved = null;
            lastModified = null;
            indexUpdateCoalescer.reset();
        }

        @Override
        public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
            if (lastAdded != null) {
                lastAdded.close();
            }
            lastAdded = added.copy();
            if (lastRemoved != null) {
                lastRemoved.close();
            }
            lastRemoved = removed.copy();
            if (lastModified != null) {
                lastModified.close();
            }
            lastModified = modified.copy();
            indexUpdateCoalescer.update(lastAdded, lastRemoved, lastModified);
            ++count;
        }
    }

    public ListenerWithGlobals newListenerWithGlobals(Table source) {
        return new ListenerWithGlobals(source);
    }

    protected class ListenerWithGlobals extends ShiftObliviousInstrumentedListenerAdapter {
        protected ListenerWithGlobals(Table source) {
            super(source, false);
            reset();
        }

        public int getCount() {
            return count;
        }

        int count;

        void reset() {
            count = 0;
            added = null;
            modified = null;
            removed = null;
        }

        @Override
        public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
            QueryTableTestBase.this.added = added.copy();
            QueryTableTestBase.this.removed = removed.copy();
            QueryTableTestBase.this.modified = modified.copy();
            ++count;
        }
    }
}
