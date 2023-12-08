/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.FailureListener;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.util.function.ThrowingRunnable;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.showWithRowSet;
import static io.deephaven.engine.util.TableTools.stringCol;

public class TestKeyedArrayBackedInputTable {

    @Rule
    public final EngineCleanup liveTableTestCase = new EngineCleanup();

    @Test
    public void testSimple() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"));

        final KeyedArrayBackedInputTable kabut = KeyedArrayBackedInputTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.addUpdateListener(failureListener);

        assertTableEquals(input, kabut);

        final InputTableUpdater inputTableUpdater = (InputTableUpdater) kabut.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(inputTableUpdater);

        final Table input2 = TableTools.newTable(stringCol("Name", "Randy"), stringCol("Employer", "USGS"));

        handleDelayedRefresh(() -> inputTableUpdater.add(input2), kabut);
        assertTableEquals(TableTools.merge(input, input2), kabut);

        final Table input3 = TableTools.newTable(stringCol("Name", "Randy"), stringCol("Employer", "Tegridy"));
        handleDelayedRefresh(() -> inputTableUpdater.add(input3), kabut);
        assertTableEquals(TableTools.merge(input, input3), kabut);


        final Table input4 = TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Cogswell"));
        handleDelayedRefresh(() -> inputTableUpdater.add(input4), kabut);
        showWithRowSet(kabut);

        assertTableEquals(TableTools.merge(input, input3, input4).lastBy("Name"), kabut);

        final Table input5 =
                TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Spacely Sprockets"));
        handleDelayedRefresh(() -> inputTableUpdater.add(input5), kabut);
        showWithRowSet(kabut);

        assertTableEquals(TableTools.merge(input, input3, input4, input5).lastBy("Name"), kabut);

        final long sizeBeforeDelete = kabut.size();
        System.out.println("KABUT.rowSet before delete: " + kabut.getRowSet());
        final Table delete1 = TableTools.newTable(stringCol("Name", "Earl"));
        handleDelayedRefresh(() -> inputTableUpdater.delete(delete1), kabut);
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

        final AppendOnlyArrayBackedInputTable aoabmt = AppendOnlyArrayBackedInputTable.make(input);
        final TableUpdateValidator validator = TableUpdateValidator.make("aoabmt", aoabmt);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.addUpdateListener(failureListener);

        assertTableEquals(input, aoabmt);

        final InputTableUpdater inputTableUpdater =
                (InputTableUpdater) aoabmt.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(inputTableUpdater);

        final Table input2 =
                TableTools.newTable(stringCol("Name", "Randy", "George"), stringCol("Employer", "USGS", "Cogswell"));

        handleDelayedRefresh(() -> inputTableUpdater.add(input2), aoabmt);
        assertTableEquals(TableTools.merge(input, input2), aoabmt);
    }

    @Test
    public void testFilteredAndSorted() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name", "Fred", "George", "Earl"),
                stringCol("Employer", "Slate Rock and Gravel", "Spacely Sprockets", "Wesayso"));

        final KeyedArrayBackedInputTable kabut = KeyedArrayBackedInputTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.addUpdateListener(failureListener);

        assertTableEquals(input, kabut);

        final Table fs = kabut.where("Name.length() == 4").sort("Name");

        final InputTableUpdater inputTableUpdater = (InputTableUpdater) fs.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(inputTableUpdater);

        final Table delete = TableTools.newTable(stringCol("Name", "Fred"));

        handleDelayedRefresh(() -> inputTableUpdater.delete(delete), kabut);
        assertTableEquals(input.where("Name != `Fred`"), kabut);
    }


    @Test
    public void testAddBack() throws Exception {
        final Table input = TableTools.newTable(stringCol("Name"), stringCol("Employer"));

        final KeyedArrayBackedInputTable kabut = KeyedArrayBackedInputTable.make(input, "Name");
        final TableUpdateValidator validator = TableUpdateValidator.make("kabut", kabut);
        final Table validatorResult = validator.getResultTable();
        final FailureListener failureListener = new FailureListener();
        validatorResult.addUpdateListener(failureListener);

        assertTableEquals(input, kabut);

        final InputTableUpdater inputTableUpdater = (InputTableUpdater) kabut.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
        TestCase.assertNotNull(inputTableUpdater);

        final Table input2 =
                TableTools.newTable(stringCol("Name", "George"), stringCol("Employer", "Spacely Sprockets"));

        handleDelayedRefresh(() -> inputTableUpdater.add(input2), kabut);
        assertTableEquals(input2, kabut);

        handleDelayedRefresh(() -> inputTableUpdater.delete(input2.view("Name")), kabut);
        assertTableEquals(input, kabut);

        handleDelayedRefresh(() -> inputTableUpdater.add(input2), kabut);
        assertTableEquals(input2, kabut);
    }

    public static void handleDelayedRefresh(final ThrowingRunnable<IOException> action,
            final BaseArrayBackedInputTable... tables) throws Exception {
        final Thread refreshThread;
        final CountDownLatch gate = new CountDownLatch(tables.length);

        Arrays.stream(tables).forEach(t -> t.setOnPendingChange(gate::countDown));
        try {
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            refreshThread = new Thread(() -> {
                // If this unexpected interruption happens, the test thread may hang in action.run()
                // indefinitely. Best to hope it's already queued the pending action and proceed with run.
                updateGraph.runWithinUnitTestCycle(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException ignored) {
                        // If this unexpected interruption happens, the test thread may hang in action.run()
                        // indefinitely. Best to hope it's already queued the pending action and proceed with run.
                    }
                    Arrays.stream(tables).forEach(BaseArrayBackedInputTable::run);
                });
            });

            refreshThread.start();
            action.run();
        } finally {
            Arrays.stream(tables).forEach(t -> t.setOnPendingChange(null));
        }
        try {
            refreshThread.join();
        } catch (InterruptedException e) {
            throw new UncheckedDeephavenException(
                    "Interrupted unexpectedly while waiting for run cycle to complete", e);
        }
    }
}
