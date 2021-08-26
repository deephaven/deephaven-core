package io.deephaven.db.v2;

import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableDiff;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import org.apache.commons.lang3.mutable.MutableInt;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * QueryTable tests can extend this to get convenient EvalNuggets, JoinIncrementors, etc.
 */
public abstract class QueryTableTestBase extends LiveTableTestCase {

    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(QueryTableTestBase.class, "CompilerTools.logEnabled", false);

    protected final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    private boolean oldLogEnabled;
    private boolean oldCheckLtm;

    private static final GenerateTableUpdates.SimulationProfile NO_SHIFT_PROFILE =
            new GenerateTableUpdates.SimulationProfile();
    static {
        NO_SHIFT_PROFILE.SHIFT_10_PERCENT_KEY_SPACE = 0;
        NO_SHIFT_PROFILE.SHIFT_10_PERCENT_POS_SPACE = 0;
        NO_SHIFT_PROFILE.SHIFT_AGGRESSIVELY = 0;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        oldLogEnabled = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
        oldCheckLtm = LiveTableMonitor.DEFAULT.setCheckTableOperations(false);
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            CompilerTools.setLogEnabled(oldLogEnabled);
            LiveTableMonitor.DEFAULT.setCheckTableOperations(oldCheckLtm);
            ChunkPoolReleaseTracking.checkAndDisable();
        }
    }

    final JoinIncrement leftStep = new JoinIncrement() {
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
    final JoinIncrement leftStepShift = new JoinIncrement() {
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
    final JoinIncrement rightStepShift = new JoinIncrement() {
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
    final JoinIncrement leftRightStep = new JoinIncrement() {
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
    final JoinIncrement leftRightStepShift = new JoinIncrement() {
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

    final JoinIncrement leftRightConcurrentStepShift = new JoinIncrement() {
        @Override
        public void step(int leftSize, int rightSize, QueryTable leftTable, QueryTable rightTable,
                TstUtils.ColumnInfo[] leftColumnInfo, TstUtils.ColumnInfo[] rightColumnInfo, EvalNuggetInterface[] en,
                Random random) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
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

    final JoinIncrement[] joinIncrementors = new JoinIncrement[] {leftStep, rightStep, leftRightStep};
    final JoinIncrement[] joinIncrementorsShift = new JoinIncrement[] {leftStep, rightStep, leftRightStep,
            leftStepShift, rightStepShift, leftRightStepShift, leftRightConcurrentStepShift};

    protected Index added;
    protected Index removed;
    protected Index modified;

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
            io.deephaven.db.tables.utils.TableTools.showWithIndex(t1);
            System.out.println(t2Name != null ? t2Name : t2);
            io.deephaven.db.tables.utils.TableTools.showWithIndex(t2);
        }
    }

    protected static class SimpleListener extends InstrumentedListenerAdapter {
        protected SimpleListener(DynamicTable source) {
            super(source, false);
            reset();
        }

        public int getCount() {
            return count;
        }

        int count;
        Index added, removed, modified;

        void reset() {
            freeResources();
            count = 0;
            added = null;
            removed = null;
            modified = null;
        }

        @Override
        public void onUpdate(Index added, Index removed, Index modified) {
            freeResources();
            // Need to clone to save IndexShiftDataExpander indices that are destroyed at the end of the LTM cycle.
            this.added = added.clone();
            this.removed = removed.clone();
            this.modified = modified.clone();
            ++count;
        }

        @Override
        public String toString() {
            return "SimpleListener{" +
                    "count=" + count +
                    ", added=" + added +
                    ", removed=" + removed +
                    ", modified=" + modified +
                    '}';
        }

        public void freeResources() {
            if (added != null) {
                added.close();
            }
            if (removed != null) {
                removed.close();
            }
            if (modified != null) {
                modified.close();
            }
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

    protected static class CoalescingListener extends InstrumentedListenerAdapter {
        Index lastAdded, lastModified, lastRemoved;
        Index.LegacyIndexUpdateCoalescer indexUpdateCoalescer = new Index.LegacyIndexUpdateCoalescer();

        protected CoalescingListener(DynamicTable source) {
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
        public void onUpdate(final Index added, final Index removed, final Index modified) {
            if (lastAdded != null) {
                lastAdded.close();
            }
            lastAdded = added.clone();
            if (lastRemoved != null) {
                lastRemoved.close();
            }
            lastRemoved = removed.clone();
            if (lastModified != null) {
                lastModified.close();
            }
            lastModified = modified.clone();
            indexUpdateCoalescer.update(lastAdded, lastRemoved, lastModified);
            ++count;
        }
    }

    protected ListenerWithGlobals newListenerWithGlobals(DynamicTable source) {
        return new ListenerWithGlobals(source);
    }

    protected class ListenerWithGlobals extends InstrumentedListenerAdapter {
        protected ListenerWithGlobals(DynamicTable source) {
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
        public void onUpdate(final Index added, final Index removed, final Index modified) {
            QueryTableTestBase.this.added = added;
            QueryTableTestBase.this.removed = removed;
            QueryTableTestBase.this.modified = modified;
            ++count;
        }
    }

}
