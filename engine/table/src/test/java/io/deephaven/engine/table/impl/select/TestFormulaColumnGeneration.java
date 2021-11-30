package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.util.ModelFileGenerator;
import io.deephaven.test.junit4.EngineCleanup;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TestFormulaColumnGeneration {
    // This is not actually a "Test", and for that reason "// @Test" is commented out.
    // The reason this is here is that this is a convenient place to put the code that
    // generates the "golden master" files tested by the "validateFiles" test. The generate
    // code only needs to be run when the formula generation code changes, and only after
    // some human believes that the code is correct. When that happens the process
    // is:
    // 1. Once the formula generation code is believed to be correct, uncomment @Test for this test case below.
    // 2. Run this test case once to generate new "golden master" files.
    // 3. Comment this "test" back out
    // 4. Confirm that the modified files pass the "validateFiles" case.
    // 4. Check in the modified "golden master" files.
    // @Test
    public void generateFiles() throws FileNotFoundException {
        final DhFormulaColumn fc = (DhFormulaColumn) getFormulaColumn();
        new ModelFileGenerator(FormulaSample.class).generateFile(fc.generateClassBody());
        new ModelFileGenerator(FormulaKernelSample.class).generateFile(fc.generateKernelClassBody());
    }

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() {
        QueryLibrary.setLibrary(QueryLibrary.makeNewLibrary("DEFAULT"));
    }

    @After
    public void tearDown() {
        QueryLibrary.resetLibrary();
    }

    @Test
    public void validateFiles() throws IOException {
        final DhFormulaColumn fc = (DhFormulaColumn) getFormulaColumn();
        new ModelFileGenerator(FormulaSample.class).validateFile(fc.generateClassBody());
        new ModelFileGenerator(FormulaKernelSample.class).validateFile(fc.generateKernelClassBody());
    }

    @NotNull
    private static FormulaColumn getFormulaColumn() {
        QueryScope.addParam("q", 7);
        final Table table = TableTools.emptyTable(10).updateView("I=i", "II=ii");
        // final DhFormulaColumn fc = new DhFormulaColumn("Value", "i + 3");
        // final DhFormulaColumn fc = new DhFormulaColumn("Value", "12345");
        // final DhFormulaColumn fc = new DhFormulaColumn("Value", "k * i * ii");
        // final DhFormulaColumn fc = new DhFormulaColumn("Value", "'2019-04-11T09:30 NY'");
        final FormulaColumn fc = FormulaColumn.createFormulaColumn("Value", "I * II + q * ii + II_[i - 1]");
        fc.initInputs(table);
        return fc;
    }
}
