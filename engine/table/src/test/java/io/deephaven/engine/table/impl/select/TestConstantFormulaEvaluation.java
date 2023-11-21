package io.deephaven.engine.table.impl.select;

import com.github.javaparser.ast.expr.Expression;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.lang.JavaExpressionParser;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.col;

public class TestConstantFormulaEvaluation {
    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private static Integer calculate(Integer x, Integer y) {
        return x * y;
    }

    @Test
    public void multiColumnSimpleFormulasTestCase() {
        final AtomicInteger atomicValue = new AtomicInteger(0);
        QueryScope.addParam("atomicValue", atomicValue);
        String[] formulas = new String[] {
                "A=k", // increasing long values
                "B=ii", // increasing long values
                "C=i", // increasing int values
                "D=7", // constant integer value of 7
                "E=\"\" + k", // increasing long values as String
                "F=4 + 3", // constant integer value of 7
                "G=(2 + 3) * 2", // constant integer value of 10
                "H=F + 3", // Formula using prev constant column as dependent variable
                "I=C * D", // Formula using two prev columns as dependent variable
                "J=2*4", // constant integer value of 8
                "K=atomicValue.getAndIncrement()", // Predictable QueryScope Usage in Formula
                "L=K*G", // Formula using two prev columns as dependent variable
                "M=K*H", // Formula using two prev columns as dependent variable
                "Exists=true", // constant boolean value of true
                "Foo=\"Bar\"", // constant String value of Bar
                "Z=K_[i-1]" // arrayAccess
        };
        final Table table = TableTools.emptyTable(7).update(formulas);
        AtomicLong atomicLong = new AtomicLong(0);
        AtomicInteger atomicInteger = new AtomicInteger(0);

        String[] columns = table.getDefinition().getColumnNamesArray();
        Assert.assertEquals("length of columns = " + formulas.length, columns.length, formulas.length);

        table.getRowSet().forAllRowKeys(key -> {
            final long expectedLongValue = atomicLong.getAndIncrement(); // for columns A and B
            ColumnSource<?> cs = table.getColumnSource("A");
            Assert.assertFalse("Col A is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "A=k"
            Assert.assertEquals("Col A expected value is an increasing long value", expectedLongValue, cs.get(key));

            cs = table.getColumnSource("B");
            Assert.assertFalse("Col B is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "B=ii"
            Assert.assertEquals("Col B expected value is an increasing long value", expectedLongValue, cs.get(key));

            final int expectedIntValue = atomicInteger.getAndIncrement();
            cs = table.getColumnSource("C");
            Assert.assertFalse("Col C is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "C=i"
            Assert.assertEquals("Col C expected value is an increasing int value", expectedIntValue, cs.get(key));

            final int expectedColDValue = 7;
            cs = table.getColumnSource("D");
            Assert.assertTrue("Col D is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "D=7"
            Assert.assertEquals("Col D expected value is " + expectedColDValue, expectedColDValue, cs.get(key));

            final String expectedColEValue = "" + expectedLongValue;
            cs = table.getColumnSource("E");
            Assert.assertFalse("Col E is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // E="" + k increasing long values as String
            Assert.assertEquals("Col E expected value is an increasing long values as String", expectedColEValue,
                    cs.get(key));

            final int expectedColFValue = 7;
            cs = table.getColumnSource("F");
            Assert.assertTrue("Col F is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "F=4 + 3", constant integer value
            Assert.assertEquals("Col F expected value is " + expectedColFValue, expectedColFValue, cs.get(key));

            final int expectedColGValue = 10;
            cs = table.getColumnSource("G");
            Assert.assertTrue("Col G is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "G=(2 + 3) * 2", constant integer value
            Assert.assertEquals("Col G expected value is " + expectedColGValue, expectedColGValue, cs.get(key));

            final int expectedColHValue = expectedColFValue + 3;
            cs = table.getColumnSource("H");
            Assert.assertFalse("Col H is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "H=F + 3", Formula using prev constant column as dependent variable
            Assert.assertEquals("Col H expected value is a constant value", expectedColHValue, cs.get(key));

            cs = table.getColumnSource("I");
            Assert.assertFalse("Col I is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "I=C * D", Formula using two prev columns as dependent variable
            Assert.assertEquals("Col I expected value is an increasing int values",
                    expectedIntValue * expectedColDValue, cs.get(key));

            final int expectedColJValue = 2 * 4;
            cs = table.getColumnSource("J");
            Assert.assertTrue("Col J is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "J=2*4", constant integer value of 8
            Assert.assertEquals("Col J expected value is a constant value", expectedColJValue, cs.get(key));

            cs = table.getColumnSource("K");
            Assert.assertFalse("Col K is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "K=atomicValue.getAndIncrement()" , atomicValue initial value set at 0
            Assert.assertEquals("Col K expected value is an increasing int value", expectedIntValue, cs.get(key));

            cs = table.getColumnSource("L");
            Assert.assertFalse("Col L is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "L=K*G", Formula using two prev columns as dependent variable
            Assert.assertEquals("Col L expected value is an increasing int values",
                    expectedIntValue * expectedColGValue, cs.get(key));

            cs = table.getColumnSource("M");
            Assert.assertFalse("Col M is not a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "M=K*H", Formula using two prev columns as dependent variable
            Assert.assertEquals("Col M expected value is an increasing int values",
                    expectedIntValue * expectedColHValue, cs.get(key));

            final boolean expectedColExistsValue = true;
            cs = table.getColumnSource("Exists");
            Assert.assertTrue("Col Exists is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "Exists=true", constant boolean value of true
            Assert.assertEquals("Col Exists expected value is a constant value", expectedColExistsValue, cs.get(key));

            final String expectedColFooValue = "Bar";
            cs = table.getColumnSource("Foo");
            Assert.assertTrue("Col Foo is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "Foo=Bar", constant String value of Bar
            Assert.assertEquals("Col Foo expected value is a constant value", expectedColFooValue, cs.get(key));

            final Integer expectedZValue = expectedIntValue == 0 ? null : expectedIntValue - 1;
            cs = table.getColumnSource("Z");
            Assert.assertFalse("Col Z is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            // "Z=K_[i-1]" arrayAccess
            Assert.assertEquals("Col Z expected value is a constant value", expectedZValue, cs.get(key));
        });
    }

    @Test
    public void constantBooleanValueTest() {
        singleColumnConstantValueFormulaTest("Exists=true", Boolean.class, true, 7, "constantBooleanValueTest");
    }

    @Test
    public void constantStringValueTest() {
        singleColumnConstantValueFormulaTest("Foo=\"Bar\"", String.class, "Bar", 5, "constantStringValueTest");
    }

    @Test
    public void constantIntValueTest() {
        singleColumnConstantValueFormulaTest("X=7", int.class, 7, 5, "constantIntValueTest");
        singleColumnConstantValueFormulaTest("X=4 + 3", int.class, 7, 7, "constantIntValueTest");
        singleColumnConstantValueFormulaTest("X=(4 - 3) * 7", int.class, 7, 7, "constantIntValueTest");
        singleColumnConstantValueFormulaTest("X=(4 + 3) * 4", int.class, 28, 7, "constantIntValueTest");
    }

    @Test
    public void constantDoubleValueTest() {
        singleColumnConstantValueFormulaTest("X=((4 + 4) / 4) + 5", double.class, 7.0d, 7, "constantDoubleValueTest");
    }

    @Test
    public void constantLongValueTest() {
        singleColumnConstantValueFormulaTest("X=7L", long.class, 7L, 7, "constantDoubleValueTest");
    }

    private <T> void singleColumnConstantValueFormulaTest(final String formula, final Class<T> columnType,
            final T columnRowValue, final int tableLength, final String description) {
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget(description)) {
            final Table source = TableTools.emptyTable(tableLength).update(formula);
            String[] columns = source.getDefinition().getColumnNamesArray();
            Assert.assertEquals("length of columns = 1", 1, columns.length);
            ColumnSource<?> cs = source.getColumnSource(columns[0]);

            Assert.assertTrue("ColumnSource is a SingleValueColumnSource", cs instanceof SingleValueColumnSource);
            source.getRowSet().forAllRowKeys(key -> {
                Assert.assertEquals(columnType, source.getColumnSource(columns[0]).getType());
                Assert.assertEquals(columnRowValue, source.getColumnSource(columns[0]).get(key));
            });
        }
    }

    @Test
    public void threeColumnConstantValueFormulaTest() {
        String[] formulas = new String[] {"X=i", "Y=7", "Z=X*Y"};
        threeColumnConstantValueFormulaTest(formulas, int.class, 7, TestConstantFormulaEvaluation::calculate, 5,
                "productOfFirstTwoColsTest");

        formulas = new String[] {"X=i", "Y=7", "Z=X+Y"};
        threeColumnConstantValueFormulaTest(formulas, int.class, 7, Integer::sum, 7, "sumOfFirstTwoColsTest");

        formulas = new String[] {"X=ii", "Y=7L", "Z=X+Y"};
        threeColumnConstantValueFormulaTest(formulas, long.class, 7L, Long::sum, 8, "sumOfFirstTwoColsTest");

        formulas = new String[] {"X=\"\" + k", "Y=\"TestConcat\"", "Z=X+Y"};
        threeColumnConstantValueFormulaTest(formulas, String.class, "TestConcat", (x, y) -> x + y, 8,
                "sumOfFirstTwoColsTest");

        formulas = new String[] {"X=\"Foo\"", "Y=\"Bar\"", "Z=X+Y"};
        threeColumnConstantValueFormulaTest(formulas, String.class, "Bar", (x, y) -> x + y, 8, "sumOfFirstTwoColsTest");
    }

    private <T> void threeColumnConstantValueFormulaTest(final String[] formulas, final Class<T> calculatedColType,
            final T expectedConstValue, final ColumnFormula<T> columnFormula, final int tableLength,
            final String description) {
        try (final SafeCloseable nugget = QueryPerformanceRecorder.getInstance().getNugget(description)) {
            final Table source = TableTools.emptyTable(tableLength).update(formulas);
            String[] columns = source.getDefinition().getColumnNamesArray();
            boolean constantValueColFound = false;
            Assert.assertEquals("length of columns = " + formulas.length, columns.length, formulas.length);
            for (int i = 0; i < formulas.length; i++) {
                ColumnSource<?> cs = source.getColumnSource(columns[i]);
                if (isConstantExpression(formulas[i])) {
                    constantValueColFound = true;
                    System.out.printf("%s is a Constant value formula, ColumnSource for Column [%s] is %s%n",
                            formulas[i], columns[i], cs);
                    Assert.assertTrue("ColumnSource is a SingleValueColumnSource ",
                            cs instanceof SingleValueColumnSource);
                } else {
                    Assert.assertFalse("ColumnSource is NOT a SingleValueColumnSource ",
                            cs instanceof SingleValueColumnSource);
                }
            }

            Assert.assertTrue("ConstantValue Column was found", constantValueColFound);

            source.getRowSet().forAllRowKeys(key -> {
                Assert.assertEquals(source.getColumnSource(columns[2]).getType(), calculatedColType);
                Assert.assertEquals("ConstantValue verification", expectedConstValue,
                        source.getColumnSource(columns[1]).get(key));
                // noinspection unchecked
                final T expected = columnFormula.calculate(
                        (T) source.getColumnSource(columns[0]).get(key),
                        (T) source.getColumnSource(columns[1]).get(key));
                Assert.assertEquals(expected, source.getColumnSource(columns[2]).get(key));
            });
        }
    }

    @Test
    public void queryScopeForAtomicIntPlusConstantFormulaTest() {
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget("queryScopeForAtomicInt")) {
            final AtomicInteger atomicValue = new AtomicInteger(1);
            QueryScope.addParam("atomicValue", atomicValue);
            String[] formulas = new String[] {
                    "X=10", // constant integer value of 10
                    "Y=atomicValue.getAndIncrement()", // Predictable QueryScope Usage in Formula
                    "Z=Y*X" // Formula using two prev columns as dependent variable
            };
            final Table source = TableTools.emptyTable(7).update(formulas);
            String[] columns = source.getDefinition().getColumnNamesArray();
            boolean constantValueColFound = false;
            Assert.assertEquals("length of columns = " + formulas.length, columns.length, formulas.length);
            for (int i = 0; i < formulas.length; i++) {
                ColumnSource<?> cs = source.getColumnSource(columns[i]);
                if (isConstantExpression(formulas[i])) {
                    constantValueColFound = true;
                    System.out.printf("%s is a Constant value formula, ColumnSource for Column [%s] is %s%n",
                            formulas[i], columns[i], cs);
                    Assert.assertTrue("ColumnSource is a SingleValueColumnSource ",
                            cs instanceof SingleValueColumnSource);
                } else {
                    Assert.assertFalse("ColumnSource is NOT a SingleValueColumnSource ",
                            cs instanceof SingleValueColumnSource);
                }
            }

            Assert.assertTrue("ConstantValue Column was found", constantValueColFound);

            AtomicInteger atomicInteger = new AtomicInteger(1);
            source.getRowSet().forAllRowKeys(key -> {
                final int expectedAtomicIntColValue = atomicInteger.getAndIncrement();
                final int expectedCalculatedColValue = 10 * expectedAtomicIntColValue;
                Assert.assertEquals("ConstantValue verification", 10, source.getColumnSource(columns[0]).get(key));
                Assert.assertEquals("AtomicInteger verification", expectedAtomicIntColValue,
                        source.getColumnSource(columns[1]).get(key));
                Assert.assertEquals("Calculate Col verification", expectedCalculatedColValue,
                        source.getColumnSource(columns[2]).get(key));
            });
        }
    }

    private interface ColumnFormula<T> {
        T calculate(T col1, T col2);
    }

    @Test
    public void constantExpressionTest() {
        Assert.assertTrue("\"Exists=true\" is a Constant Value Expression", isConstantExpression("Exists=true"));
        Assert.assertTrue("\"Foo=\"Bar\"\" is a Constant Value Expression", isConstantExpression("Foo=\"Bar\""));
        Assert.assertTrue("\"X=7\" is a Constant Value Expression", isConstantExpression("X=7"));
        Assert.assertTrue("\"U=4 + 3\" is a Constant Value Expression", isConstantExpression("U=4 + 3"));
        Assert.assertTrue("\"A=4 + 3 + 3\" is a Constant Value Expression", isConstantExpression("A=4 + 3 + 3"));
        Assert.assertTrue("\"B=4 / (3 + 3)\" is a Constant Value Expression", isConstantExpression("B=4 / (3 + 3)"));
        Assert.assertTrue("\"C=(4 + 3) + (3 * (5 + (8 - 6)))\" is a Constant Value Expression",
                isConstantExpression("C=(4 + 3) + (3 * (5 + (8 - 6)))"));
        Assert.assertTrue("\"D=((4 + 3) - (7 -95) + (3 * (5 + (8 - 6))))\" is a Constant Value Expression",
                isConstantExpression("D=((4 + 3) - (7 -95) + (3 * (5 + (8 - 6))))"));
    }

    @Test
    public void notConstantExpressionTest() {
        Assert.assertFalse("\"W=k\" is not a Constant Value Expression", isConstantExpression("W=k"));
        Assert.assertFalse("\"M=ii\" is not a Constant Value Expression", isConstantExpression("M=ii"));
        Assert.assertFalse("\"N=i\" is not a Constant Value Expression", isConstantExpression("N=i"));
        Assert.assertFalse("\"N=i\" is not a Constant Value Expression", isConstantExpression("P=4 + a"));
        Assert.assertFalse("\"Z=Y_[i-1]\" is not a Constant Value Expression", isConstantExpression("Z=Y_[i-1]"));
        Assert.assertFalse("\"Y=random.nextInt(1000000)\" is not a Constant Value Expression",
                isConstantExpression("Y=random.nextInt(1000000)"));
        Assert.assertFalse("\"e=((4 + 3) - (7 -95) + (3 * (5 + (g - 8))))\" is not a Constant Value Expression",
                isConstantExpression("e=((4 + 3) - (7 -95) + (3 * (5 + (g - 8))))"));
        Assert.assertFalse("\"f=((4 + 3) - (7 -95) + (3 * (5 + (8 - g))))\" is not a Constant Value Expression",
                isConstantExpression("f=((4 + 3) - (7 -95) + (3 * (5 + (8 - g))))"));
    }

    private boolean isConstantExpression(String expression) {
        if (expression == null || expression.trim().length() == 0) {
            return false;
        }
        Expression expr = JavaExpressionParser.parseExpression(expression);
        return JavaExpressionParser.isConstantValueExpression(expr);
    }

    @Test
    public void testRefreshingTableForConstantFormulaColumnSource() {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", 'a', 'b', 'c'));
        final String[] formulas = new String[] {"x = x * 2", "z = y", "u=7"};
        QueryTable table2 = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> (QueryTable) table.select(formulas));
        Set<String> expectedConstValueColumns = Collections.singleton("u");
        Integer[] expectedConstValues = new Integer[] {7};
        checkConstantFormula(table2, expectedConstValueColumns, expectedConstValues, int.class);

        final String[] formulas2 = new String[] {"x = x * 2", "z1 = z", "u1=u", "u2=u1 * 2"};
        QueryTable table3 = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> (QueryTable) table2.select(formulas2));
        Set<String> expectedConstValueColumns2 = Collections.singleton("u1");
        Integer[] expectedConstValues2 = new Integer[] {7};
        // verify parent constant value ColumnSource is same when inherited as is
        Assert.assertSame(table3.getColumnSource("u1"), table2.getColumnSource("u"));
        checkConstantFormula(table3, expectedConstValueColumns2, expectedConstValues2, int.class);
    }

    @SuppressWarnings("SameParameterValue")
    private <T> void checkConstantFormula(final Table source, final Set<String> expectedConstValueColumns,
            final T[] expectedConstValues, final Class<T> calculatedColType) {
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget("queryScopeForAtomicInt")) {
            int count = 0;
            int[] constantColIndex = new int[expectedConstValues.length];
            String[] columns = source.getDefinition().getColumnNamesArray();

            for (int ii = 0; ii < columns.length; ii++) {
                final ColumnSource<?> cs = source.getColumnSource(columns[ii]);
                if (expectedConstValueColumns.contains(columns[ii])) {
                    constantColIndex[count++] = ii;
                    Assert.assertTrue("ColumnSource is a SingleValueColumnSource ",
                            cs instanceof SingleValueColumnSource);
                    Assert.assertEquals("ColumnType ", calculatedColType, cs.getType());
                } else {
                    Assert.assertFalse("ColumnSource is NOT a SingleValueColumnSource ",
                            cs instanceof SingleValueColumnSource);
                }
            }

            Assert.assertEquals("ConstantValue Columns found, count matches expectedConstValues count", count,
                    expectedConstValues.length);

            if (expectedConstValues.length == 0) {
                return;
            }

            source.getRowSet().forAllRowKeys(key -> {
                for (int i = 0; i < constantColIndex.length; i++) {
                    Assert.assertEquals("ConstantValue verification", expectedConstValues[i],
                            source.getColumnSource(columns[constantColIndex[i]]).get(key));
                }
            });
        }
    }
}
