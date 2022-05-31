package io.deephaven.engine.table.impl.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.SleepUtil;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.config.InputTableStatusListener;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.engine.table.impl.FailureListener;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.util.FunctionalInterfaces;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.util.TableTools.showWithRowSet;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;

public class TestKeyedArrayBackedMutableTable {

    @Rule
    public final EngineCleanup liveTableTestCase = new EngineCleanup();

    @Test
    public void testSimple() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"));

        final KeyedArrayBackedMutableTable kabut = KeyedArrayBackedMutableTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.listenForUpdates(failureListener);

        assertTableEquals(input, kabut);

        final MutableInputTable mutableInputTable = (MutableInputTable) kabut.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(mutableInputTable);

        final Table input2 = TableTools.newTable(stringCol("Name", "Randy"), stringCol("Employer", "USGS"));

        handleDelayedRefresh(kabut, () -> mutableInputTable.add(input2));
        assertTableEquals(TableTools.merge(input, input2), kabut);

        final Table input3 = TableTools.newTable(stringCol("Name", "Randy"), stringCol("Employer", "Tegridy"));
        handleDelayedRefresh(kabut, () -> mutableInputTable.add(input3));
        assertTableEquals(TableTools.merge(input, input3), kabut);


        final Table input4 = TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Cogswell"));
        handleDelayedRefresh(kabut, () -> mutableInputTable.add(input4));
        showWithRowSet(kabut);

        assertTableEquals(TableTools.merge(input, input3, input4).lastBy("Name"), kabut);

        final Table input5 =
                TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Spacely Sprockets"));
        handleDelayedRefresh(kabut, () -> mutableInputTable.add(input5));
        showWithRowSet(kabut);

        assertTableEquals(TableTools.merge(input, input3, input4, input5).lastBy("Name"), kabut);

        final long sizeBeforeDelete = kabut.size();
        System.out.println("KABUT.rowSet before delete: " + kabut.getRowSet());
        final Table delete1 = TableTools.newTable(stringCol("Name", "Earl"));
        handleDelayedRefresh(kabut, () -> mutableInputTable.delete(delete1));
        System.out.println("KABUT.rowSet after delete: " + kabut.getRowSet());
        final long sizeAfterDelete = kabut.size();
        TestCase.assertEquals(sizeBeforeDelete - 1, sizeAfterDelete);

        showWithRowSet(kabut);

        final Table expected = TableTools.merge(
                TableTools.merge(input, input3, input4, input5).update("Deleted=false"),
                delete1.update("Employer=(String)null", "Deleted=true"))
                .lastBy("Name").where("Deleted=false").dropColumns("Deleted");
        showWithRowSet(expected);

        assertTableEquals(expected, kabut);
    }

    @Test
    public void testAppendOnly() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"));

        final AppendOnlyArrayBackedMutableTable aoabmt = AppendOnlyArrayBackedMutableTable.make(input);
        final TableUpdateValidator validator = TableUpdateValidator.make("aoabmt", aoabmt);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.listenForUpdates(failureListener);

        assertTableEquals(input, aoabmt);

        final MutableInputTable mutableInputTable =
                (MutableInputTable) aoabmt.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(mutableInputTable);

        final Table input2 =
                TableTools.newTable(stringCol("Name", "Randy", "George"), stringCol("Employer", "USGS", "Cogswell"));

        handleDelayedRefresh(aoabmt, () -> mutableInputTable.add(input2));
        assertTableEquals(TableTools.merge(input, input2), aoabmt);
    }

    @Test
    public void testFilteredAndSorted() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"));

        final KeyedArrayBackedMutableTable kabut = KeyedArrayBackedMutableTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.listenForUpdates(failureListener);

        assertTableEquals(input, kabut);

        final Table fs = kabut.where("Name.length() == 4").sort("Name");

        final MutableInputTable mutableInputTable = (MutableInputTable) fs.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(mutableInputTable);

        final Table delete = TableTools.newTable(stringCol("Name", "Fred"));

        handleDelayedRefresh(kabut, () -> mutableInputTable.delete(delete));
        assertTableEquals(input.where("Name != `Fred`"), kabut);
    }

    @Test
    public void testAddRows() throws Throwable {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"));

        final KeyedArrayBackedMutableTable kabut = KeyedArrayBackedMutableTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.listenForUpdates(failureListener);

        assertTableEquals(input, kabut);

        final MutableInputTable mutableInputTable = (MutableInputTable) kabut.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(mutableInputTable);

        final Table input2 = TableTools.newTable(stringCol("Name", "Randy"), stringCol("Employer", "USGS"));

        final Map<String, Object> randyMap =
                CollectionUtil.mapFromArray(String.class, Object.class, "Name", "Randy", "Employer", "USGS");
        final TestStatusListener listener = new TestStatusListener();
        mutableInputTable.addRow(randyMap, true, listener);
        SleepUtil.sleep(100);
        listener.assertIncomplete();
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(kabut::run);
        assertTableEquals(TableTools.merge(input, input2), kabut);
        listener.waitForCompletion();
        listener.assertSuccess();

        // TODO: should we throw the exception from the initial palce, should we defer edit checking to the UGP which
        // would make it consistent, but also slower to produce errors and uglier for reporting?
        final TestStatusListener listener2 = new TestStatusListener();
        final Map<String, Object> randyMap2 =
                CollectionUtil.mapFromArray(String.class, Object.class, "Name", "Randy", "Employer", "Tegridy");
        mutableInputTable.addRow(randyMap2, false, listener2);
        SleepUtil.sleep(100);
        listener2.assertIncomplete();
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(kabut::run);
        assertTableEquals(TableTools.merge(input, input2), kabut);
        listener2.waitForCompletion();
        listener2.assertFailure(IllegalArgumentException.class, "Can not edit keys Randy");
    }

    @Test
    public void testAddBack() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name"), stringCol("Employer"));

        final KeyedArrayBackedMutableTable kabut = KeyedArrayBackedMutableTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.listenForUpdates(failureListener);

        assertTableEquals(input, kabut);

        final MutableInputTable mutableInputTable = (MutableInputTable) kabut.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(mutableInputTable);

        final Table input2 =
                TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Spacely Sprockets"));

        handleDelayedRefresh(kabut, () -> mutableInputTable.add(input2));
        assertTableEquals(input2, kabut);

        handleDelayedRefresh(kabut, () -> mutableInputTable.delete(input2.view("Name")));
        assertTableEquals(input, kabut);

        handleDelayedRefresh(kabut, () -> mutableInputTable.add(input2));
        assertTableEquals(input2, kabut);
    }

    @Test
    public void testSetRows() {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"),
                stringCol("Spouse", "Wilma", "Jane", "Fran"));

        final KeyedArrayBackedMutableTable kabut = KeyedArrayBackedMutableTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.listenForUpdates(failureListener);

        assertTableEquals(input, kabut);

        final MutableInputTable mutableInputTable = (MutableInputTable) kabut.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(mutableInputTable);

        final Table defaultValues = input.where("Name=`George`");
        final Table ex2 = TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Cogswell"),
                stringCol("Spouse", "Jane"));

        final Map<String, Object> cogMap =
                CollectionUtil.mapFromArray(String.class, Object.class, "Name", "George", "Employer", "Cogswell");
        mutableInputTable.setRow(defaultValues, 0, cogMap);
        SleepUtil.sleep(100);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(kabut::run);
        assertTableEquals(TableTools.merge(input, ex2).lastBy("Name"), kabut);
    }

    private static class TestStatusListener implements InputTableStatusListener {
        boolean success = false;
        Throwable error = null;

        @Override
        public synchronized void onError(Throwable t) {
            if (success || error != null) {
                throw new IllegalStateException("Can not complete listener twice!");
            }
            error = t;
            notifyAll();
        }

        @Override
        public synchronized void onSuccess() {
            if (success || error != null) {
                throw new IllegalStateException("Can not complete listener twice!");
            }
            success = true;
            notifyAll();
        }

        private synchronized void assertIncomplete() {
            TestCase.assertFalse(success);
            TestCase.assertNull(error);
        }

        private void waitForCompletion() throws InterruptedException {
            synchronized (this) {
                while (!success && error == null) {
                    wait();
                }
            }
        }

        private synchronized void assertSuccess() throws Throwable {
            if (!success) {
                throw error;
            }
        }

        private synchronized void assertFailure(@NotNull final Class<? extends Throwable> errorClass,
                @Nullable final String errorMessage) {
            TestCase.assertFalse(success);
            TestCase.assertNotNull(error);
            TestCase.assertTrue(errorClass.isAssignableFrom(error.getClass()));
            if (errorMessage != null) {
                TestCase.assertEquals(errorMessage, error.getMessage());
            }
        }
    }

    private void handleDelayedRefresh(final BaseArrayBackedMutableTable table,
            final FunctionalInterfaces.ThrowingRunnable<IOException> action) throws Exception {
        final Thread refreshThread;
        final CountDownLatch gate = new CountDownLatch(1);

        table.setOnPendingChange(gate::countDown);
        try {
            refreshThread = new Thread(() -> {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException ignored) {
                        // If this unexpected interruption happens, the test thread may hang in action.run()
                        // indefinitely. Best to hope it's already queued the pending action and proceed with run.
                    }
                    table.run();
                });
            });

            refreshThread.start();
            action.run();
        } finally {
            table.setOnPendingChange(null);
        }
        try {
            refreshThread.join();
        } catch (InterruptedException e) {
            throw new UncheckedDeephavenException(
                    "Interrupted unexpectedly while waiting for run cycle to complete", e);
        }
    }
}
