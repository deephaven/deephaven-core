package io.deephaven.db.v2.select;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.utils.ModelFileGenerator;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TestConditionFilterGeneration {
    @Before
    public void setUp() {
        QueryLibrary.setLibrary(QueryLibrary.makeNewLibrary("DEFAULT"));
    }

    @After
    public void tearDown() {
        QueryLibrary.resetLibrary();
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
        QueryScope.getScope().putParam("p1", 10);
        QueryScope.getScope().putParam("p2", (float) 10);
        QueryScope.getScope().putParam("p3", "10");
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
