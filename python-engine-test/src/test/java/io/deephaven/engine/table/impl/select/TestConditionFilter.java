package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.PythonDeephavenSession;
import io.deephaven.engine.util.PythonScopeJpyImpl;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.jpy.PythonTest;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jpy.PyInputMode;
import org.jpy.PyModule;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static io.deephaven.engine.table.impl.select.FormulaTestUtil.*;
import static org.jpy.PyLib.getMainGlobals;
import static org.junit.Assert.fail;

@Ignore // TODO (deephaven-core#734)
public class TestConditionFilter extends PythonTest {
    static {
        if (ProcessEnvironment.tryGet() == null) {
            ProcessEnvironment.basicInteractiveProcessInitialization(Configuration.getInstance(),
                    TestConditionFilter.class.getCanonicalName(), new StreamLoggerImpl(System.out, LogLevel.INFO));
        }
    }

    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(TestConditionFilter.class, "CompilerTools.logEnabled", false);

    private final Table testDataTable;
    private boolean compilerToolsLogEnabledInitial = false;

    public TestConditionFilter() {
        testDataTable = getTestDataTable();

        setUpQueryLibrary();
        setUpQueryScope();
    }

    @Before
    public void setUp() throws Exception {
        if (ProcessEnvironment.tryGet() == null) {
            ProcessEnvironment.basicInteractiveProcessInitialization(Configuration.getInstance(),
                    PythonMatchFilterTest.class.getCanonicalName(), new StreamLoggerImpl(System.out, LogLevel.INFO));
        }
        compilerToolsLogEnabledInitial = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
    }

    @After
    public void tearDown() throws Exception {
        CompilerTools.setLogEnabled(compilerToolsLogEnabledInitial);
    }

    @Test
    public void testTrueFalse() {
        String expression;
        Predicate<Map<String, Object>> test;

        expression = "true";
        test = (colValues) -> true;
        check(expression, test, true);

        expression = "false";
        test = (colValues) -> false;
        check(expression, test, true);
    }

    @Test
    public void testObjectConstruction() {
        String expression;
        Predicate<Map<String, Object>> test;

        expression = "new Boolean(true)";
        test = (colValues) -> true;
        check(expression, test);

        expression = "new Boolean(false)";
        test = (colValues) -> false;
        check(expression, test);
    }

    @Test
    public void testRuntimeException() {
        checkExpectingEvaluationException("((Boolean) null)", "NullPointerException");
        checkExpectingEvaluationException("Integer.parseInt(\"this is not an integer\") != null",
                "NumberFormatException");
    }

    @Test
    public void testBadExpressionType() {
        checkExpectingCompilationException("0", "boolean required");
        checkExpectingCompilationException("IntCol", "boolean required");
    }

    @Test
    public void testMiscCompilationExceptions() {
        checkExpectingCompilationException("nonExistentVariableOrClass",
                "Cannot find variable or class nonExistentVariableOrClass");
        checkExpectingCompilationException("Integer.noSuchMethod()", "Cannot find method noSuchMethod()");
    }

    @Test
    public void testNullFilters() {
        String expression = "!isNull(IntCol)";
        Predicate<Map<String, Object>> test = (colValues) -> colValues.get("IntCol") != null;
        check(expression, test);

        expression = "isNull(IntCol)";
        test = (colValues) -> colValues.get("IntCol") == null;
        check(expression, test);
    }

    @Test
    public void testComparison() {
        String expression;
        Predicate<Map<String, Object>> test;

        { // LESS THAN
            expression = "myShortObj < ShortCol";
            test = (colValues) -> QueryLanguageFunctionUtils.less(
                    QueryLanguageFunctionUtils.shortCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.shortCast(colValues.get("ShortCol")));
            check(expression, test, true, false);

            expression = "myIntObj < IntCol";
            test = (colValues) -> QueryLanguageFunctionUtils.less(
                    QUERYSCOPE_OBJ_BASE_VALUE,
                    QueryLanguageFunctionUtils.intCast(colValues.get("IntCol")));
            check(expression, test, true);

            expression = "myLongObj < LongCol";
            test = (colValues) -> QueryLanguageFunctionUtils.less(
                    QueryLanguageFunctionUtils.longCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.longCast(colValues.get("LongCol")));
            check(expression, test, true);

            expression = "myFloatObj < FloatCol";
            test = (colValues) -> QueryLanguageFunctionUtils.less(
                    QueryLanguageFunctionUtils.floatCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.floatCast(colValues.get("FloatCol")));
            check(expression, test, true);

            expression = "myDoubleObj < DoubleCol";
            test = (colValues) -> QueryLanguageFunctionUtils.less(
                    QueryLanguageFunctionUtils.doubleCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.doubleCast(colValues.get("DoubleCol")));
            check(expression, test, true);
        }

        { // GREATER THAN
            expression = "myShortObj > ShortCol";
            test = (colValues) -> QueryLanguageFunctionUtils.greater(
                    QueryLanguageFunctionUtils.shortCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.shortCast(colValues.get("ShortCol")));
            check(expression, test, true);

            expression = "myIntObj > IntCol";
            test = (colValues) -> QueryLanguageFunctionUtils.greater(
                    QUERYSCOPE_OBJ_BASE_VALUE,
                    QueryLanguageFunctionUtils.intCast(colValues.get("IntCol")));
            check(expression, test, true);

            expression = "myLongObj > LongCol";
            test = (colValues) -> QueryLanguageFunctionUtils.greater(
                    QueryLanguageFunctionUtils.longCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.longCast(colValues.get("LongCol")));
            check(expression, test, true);

            expression = "myFloatObj > FloatCol";
            test = (colValues) -> QueryLanguageFunctionUtils.greater(
                    QueryLanguageFunctionUtils.floatCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.floatCast(colValues.get("FloatCol")));
            check(expression, test, true);

            expression = "myDoubleObj > DoubleCol";
            test = (colValues) -> QueryLanguageFunctionUtils.greater(
                    QueryLanguageFunctionUtils.doubleCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.doubleCast(colValues.get("DoubleCol")));
            check(expression, test, true);
        }

        { // EQUAL
            expression = "myShortObj == ShortCol";
            test = (colValues) -> QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.shortCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.shortCast(colValues.get("ShortCol")));
            check(expression, test, true);

            expression = "myIntObj == IntCol";
            test = (colValues) -> QueryLanguageFunctionUtils.eq(
                    QUERYSCOPE_OBJ_BASE_VALUE,
                    QueryLanguageFunctionUtils.intCast(colValues.get("IntCol")));
            check(expression, test, true);

            expression = "myLongObj == LongCol";
            test = (colValues) -> QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.longCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.longCast(colValues.get("LongCol")));
            check(expression, test, true);

            expression = "myFloatObj == FloatCol";
            test = (colValues) -> QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.floatCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.floatCast(colValues.get("FloatCol")));
            check(expression, test, true);

            expression = "myDoubleObj == DoubleCol";
            test = (colValues) -> QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.doubleCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.doubleCast(colValues.get("DoubleCol")));
            check(expression, test, true);
        }

        { // NOT EQUAL
            expression = "myShortObj != ShortCol";
            test = (colValues) -> !QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.shortCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.shortCast(colValues.get("ShortCol")));
            check(expression, test, true);

            expression = "myIntObj != IntCol";
            test = (colValues) -> !QueryLanguageFunctionUtils.eq(
                    QUERYSCOPE_OBJ_BASE_VALUE,
                    QueryLanguageFunctionUtils.intCast(colValues.get("IntCol")));
            check(expression, test, true);

            expression = "myLongObj != LongCol";
            test = (colValues) -> !QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.longCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.longCast(colValues.get("LongCol")));
            check(expression, test, true);

            expression = "myFloatObj != FloatCol";
            test = (colValues) -> !QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.floatCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.floatCast(colValues.get("FloatCol")));
            check(expression, test, true);

            expression = "myDoubleObj != DoubleCol";
            test = (colValues) -> !QueryLanguageFunctionUtils.eq(
                    QueryLanguageFunctionUtils.doubleCast(QUERYSCOPE_OBJ_BASE_VALUE),
                    QueryLanguageFunctionUtils.doubleCast(colValues.get("DoubleCol")));
            check(expression, test, true);
        }
    }

    @Test
    public void testLoadNumpyTwice() {
        Assert.assertNotNull(PyModule.importModule("deephaven/numba"));
        Assert.assertNotNull(PyModule.importModule("numpy"));
        Assert.assertNotNull(PyModule.importModule("deephaven.lang.vectorize_simple"));
        Assert.assertNotNull(PyModule.importModule("deephaven/numba"));
        Assert.assertNotNull(PyModule.importModule("numpy"));
        Assert.assertNotNull(PyModule.importModule("deephaven.lang.vectorize_simple"));
    }

    @Test
    public void testPython() {
        PyObject.executeCode("from numba.npyufunc import vectorize\n" +
                "@vectorize\n" +
                "def testf(a, b, c):\n" +
                "    return a + b < c\n" +
                "\n", PyInputMode.SCRIPT);

        check("testf(IntCol,IntCol*2,IntCol+2)", m -> {
            Integer ic = (Integer) m.get("IntCol");
            if (ic == null) {
                return true;
            }
            return ic + ic * 2 < ic + 2;
        }, true, false);
    }

    @Test
    public void testIIIK() {

        check("i > 1", m -> {
            Integer i = (Integer) m.get("actualI");
            return i > 1;
        }, true, false);
        check("i <= 1", m -> {
            Integer i = (Integer) m.get("actualI");
            return i <= 1;
        }, true, false);
        check("k > 1", m -> {
            Long k = (Long) m.get("actualK");
            return k > 1;
        }, true, false);
        check("k <= 1", m -> {
            Long k = (Long) m.get("actualK");
            return k <= 1;
        }, true, false);
        check("ii > 1", m -> {
            Long ii = (Long) m.get("actualII");
            return ii > 1;
        }, true, false);
        check("ii <= 1", m -> {
            Long ii = (Long) m.get("actualII");
            return ii <= 1;
        }, true, false);
    }


    /**
     * Ensure that a {@link ConditionFilter} with the given {@code expression} {@link #testDataTable} filtered by a
     * ConditionF
     *
     * @param expression the conditional expression to check
     * @param testPredicate the predicate over a map of column values to compare with the expression
     */
    private void check(String expression, Predicate<Map<String, Object>> testPredicate) {
        check(expression, testPredicate, false, true);
    }

    private void check(String expression, Predicate<Map<String, Object>> testPredicate, boolean testPython) {
        check(expression, testPredicate, testPython, true);
    }

    private void check(String expression, Predicate<Map<String, Object>> testPredicate, boolean testPython,
            boolean testNative) {
        final RowSetBuilderSequential keepBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential dropBuilder = RowSetFactory.builderSequential();

        final Map<String, ? extends ColumnSource> sourcesMap =
                testDataTable.updateView("actualI = i", "actualII = ii", "actualK = k").getColumnSourceMap();

        for (final RowSet.Iterator it = testDataTable.getRowSet().iterator(); it.hasNext();) {
            final long idx = it.nextLong();
            final Map<String, Object> rowMap = new HashMap<>(sourcesMap.size());
            for (Map.Entry<String, ? extends ColumnSource> entry : sourcesMap.entrySet()) {
                rowMap.put(
                        entry.getKey(),
                        entry.getValue().get(idx));
            }
            if (testPredicate.test(rowMap)) {
                keepBuilder.appendKey(idx);
            } else {
                dropBuilder.appendKey(idx);
            }
        }

        final RowSet keepRowSet = keepBuilder.build();
        final RowSet dropRowSet = dropBuilder.build();

        if (testNative) {
            validate(expression, keepRowSet, dropRowSet, FormulaParserConfiguration.Deephaven);
        }
        if (testPython) {
            QueryScope currentScope = QueryScope.getScope();
            try {
                if (pythonScope == null) {
                    pythonScope = new PythonDeephavenSession(new PythonScopeJpyImpl(getMainGlobals().asDict()))
                            .newQueryScope();
                    QueryScope.setScope(pythonScope);
                }
                for (QueryScopeParam param : currentScope.getParams(currentScope.getParamNames())) {
                    pythonScope.putParam(param.getName(), param.getValue());
                }
                expression = expression.replaceAll("true", "True").replaceAll("false", "False");
                validate(expression, keepRowSet, dropRowSet, FormulaParserConfiguration.Numba);
            } finally {
                QueryScope.setScope(currentScope);
            }
        }

    }

    private void validate(String expression, RowSet keepRowSet, RowSet dropRowSet, FormulaParserConfiguration parser) {
        final RowSet filteredRowSet = initCheck(expression, parser);

        Require.eq(keepRowSet.size(), "keepRowSet.size()", filteredRowSet.size(), "filteredRowSet.size()");
        Require.eq(keepRowSet.intersect(filteredRowSet).size(), "keepRowSet.intersect(filteredRowSet).size()",
                filteredRowSet.size(), "filteredRowSet.size()");
        Require.eqZero(dropRowSet.intersect(filteredRowSet).size(), "dropRowSet.intersect(filteredRowSet).size()");
    }


    private void checkExpectingEvaluationException(String expression, String expectedCauseMessage) {
        try {
            initCheck(expression, FormulaParserConfiguration.Deephaven);
            fail("Should have thrown an exception");
        } catch (FormulaEvaluationException ex) {
            if (!ex.getMessage().contains(expectedCauseMessage)
                    && !ex.getCause().getMessage().contains(expectedCauseMessage)) // check the cause, since all
                                                                                   // exceptions during filter
                                                                                   // evaluation are caught
            {
                fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
            }
        }
    }

    private void checkExpectingCompilationException(String expression, String expectedCauseMessage) {
        try {
            initCheck(expression, FormulaParserConfiguration.Deephaven);
            fail("Should have thrown an exception");
        } catch (FormulaCompilationException ex) {
            if (!ex.getMessage().contains(expectedCauseMessage)
                    && !ex.getCause().getMessage().contains(expectedCauseMessage)) // check the cause, since all
                                                                                   // exceptions during filter init are
                                                                                   // caught
            {
                fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
            }
        }

    }


    private RowSet initCheck(String expression, FormulaParserConfiguration parser) {
        final WhereFilter conditionFilter = ConditionFilter.createConditionFilter(expression, parser);
        conditionFilter.init(testDataTable.getDefinition());
        return conditionFilter.filter(testDataTable.getRowSet().copy(), testDataTable.getRowSet(), testDataTable,
                false);
    }

}
