package io.deephaven.engine.util;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.jpy.JpyInit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;

/**
 * Test various Jpy related overloading methods.
 */
@Ignore // TODO (deephaven-core#734)
public class TestWorkerPythonEnvironment extends BaseArrayTestCase {

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (ProcessEnvironment.tryGet() == null) {
            ProcessEnvironment.basicInteractiveProcessInitialization(Configuration.getInstance(),
                    TestWorkerPythonEnvironment.class.getCanonicalName(),
                    new StreamLoggerImpl(System.out, LogLevel.INFO));
        }
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    public void testNumpyImport() {
        WorkerPythonEnvironment.DEFAULT.eval("import numpy");
    }

    public void testTimeTable() throws IOException {
        WorkerPythonEnvironment.DEFAULT.eval("tt = timeTable(\"00:00:01\")");
        Object result = WorkerPythonEnvironment.DEFAULT.getValue("tt");
        assertTrue(result instanceof Table);
        Table tt = (Table) result;
        TableTools.show(tt);
    }

    public void testEmptyTable() throws IOException {
        WorkerPythonEnvironment.DEFAULT
                .eval("TableTools = jpy.get_type(\"io.deephaven.engine.util.TableTools\")");
        WorkerPythonEnvironment.DEFAULT.eval("et = TableTools.emptyTable(2).update(\"A=k\")");
        Object result = WorkerPythonEnvironment.DEFAULT.getValue("et");
        assertTrue(result instanceof Table);
        Table et = (Table) result;
        TableTools.show(et);
    }

    public void testUpdateList() throws IOException {
        WorkerPythonEnvironment.DEFAULT
                .eval("TableTools = jpy.get_type(\"io.deephaven.engine.util.TableTools\")");
        WorkerPythonEnvironment.DEFAULT.eval("et = TableTools.emptyTable(2).update([\"A=k\", \"B=i*2\"])");
        Object result = WorkerPythonEnvironment.DEFAULT.getValue("et");
        assertTrue(result instanceof Table);
        Table et = (Table) result;
        TableTools.show(et);
    }

    public void testUpdateVarArgs() throws IOException {
        WorkerPythonEnvironment.DEFAULT
                .eval("TableTools = jpy.get_type(\"io.deephaven.engine.util.TableTools\")");
        WorkerPythonEnvironment.DEFAULT.eval("et = TableTools.emptyTable(2).update(\"A=k\", \"B=i*2\")");
        Object result = WorkerPythonEnvironment.DEFAULT.getValue("et");
        assertTrue(result instanceof Table);
        Table et = (Table) result;
        TableTools.show(et);
    }


    public void testScript() throws IOException, InterruptedException, TimeoutException {
        JpyInit.init();
        final PythonEvaluator evaluator = PythonEvaluatorJpy.withGlobalCopy();
        final String filename = "/tmp/_not_existent_file.py";

        final File file = new File(filename);
        assertFalse(file.exists());

        try {
            evaluator.runScript(filename);
            // we should never get here
            assertFalse(true);
        } catch (FileNotFoundException fnfe) {
            assertEquals(fnfe.getMessage(), filename);
        }

        System.out.println("Run script done.");
    }
}
