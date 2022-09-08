/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.util.ModelFileGenerator;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TestConditionFilterGeneration {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private SafeCloseable executionContext;

    @Before
    public void setUp() {
        executionContext = ExecutionContext.newBuilder()
                .newQueryLibrary("DEFAULT")
                .captureQueryCompiler()
                .captureQueryScope()
                .build().open();

    }

    // @Test
    public void generateFile() throws FileNotFoundException {
        new ModelFileGenerator(FilterKernelSample.class).generateFile(getClassDefString());
    }

    // @Test
    public void generateArrayFile() throws FileNotFoundException {
        new ModelFileGenerator(FilterKernelArraySample.class).generateFile(getArrayClassDefString());
    }

    @Test
    public void validateFile() throws IOException {
        new ModelFileGenerator(FilterKernelSample.class).validateFile(getClassDefString());
    }

    @Test
    public void validateArrayFile() throws IOException {
        new ModelFileGenerator(FilterKernelArraySample.class).validateFile(getArrayClassDefString());
    }

    @NotNull
    private static String getClassDefString() {
        ExecutionContext.getContext().getQueryScope().putParam("p1", 10);
        ExecutionContext.getContext().getQueryScope().putParam("p2", (float) 10);
        ExecutionContext.getContext().getQueryScope().putParam("p3", "10");
        final Table t = TableTools.emptyTable(10).select("v1 = (short)1", "v2 = 1.1");

        final ConditionFilter conditionFilter =
                (ConditionFilter) ConditionFilter.createConditionFilter("`foo`.equals((p1+p2+v1+v2) + p3)");
        conditionFilter.init(t.getDefinition());
        return conditionFilter.getClassBodyStr();
    }

    @NotNull
    private static String getArrayClassDefString() {
        final Table t = TableTools.emptyTable(10).select("v1 = (short)1", "v2 = 1.1");

        final ConditionFilter conditionFilter =
                (ConditionFilter) ConditionFilter.createConditionFilter("v1_.size() == v2_.size()");
        conditionFilter.init(t.getDefinition());
        return conditionFilter.getClassBodyStr();
    }
}
