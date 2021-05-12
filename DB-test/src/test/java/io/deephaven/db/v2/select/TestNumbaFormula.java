package io.deephaven.db.v2.select;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.jpy.PythonTest;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.deephaven.db.v2.select.FormulaParserConfiguration.dh;
import static io.deephaven.db.v2.select.FormulaParserConfiguration.nb;
import static junit.framework.TestCase.assertEquals;

@Ignore
public class TestNumbaFormula extends PythonTest {
    static {
        if (ProcessEnvironment.tryGet() == null) {
            ProcessEnvironment.basicInteractiveProcessInitialization(Configuration.getInstance(), TestNumbaFormula.class.getCanonicalName(), new StreamLoggerImpl(System.out, LogLevel.INFO));
        }
    }

    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance().getBooleanForClassWithDefault(TestNumbaFormula.class, "CompilerTools.logEnabled", false);

    private FormulaParserConfiguration prevParserSettings;

    private boolean compilerToolsLogEnabledInitial = false;

    @Before
    public void setUp() throws Exception {
        compilerToolsLogEnabledInitial = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
    }


    @After
    public void tearDown() throws Exception {
        CompilerTools.setLogEnabled(compilerToolsLogEnabledInitial);
    }

    @Before
    public void setParser() {
        prevParserSettings = FormulaParserConfiguration.parser;
        FormulaParserConfiguration.setParser(FormulaParserConfiguration.Numba);
    }

    @After
    public void restoreParserSettings() {
        FormulaParserConfiguration.parser = prevParserSettings;
    }

    private static Table getSumTable(int size, String type) {

        return TableTools.emptyTable(size).select(
                dh("A = (" + type + ") (k % 17)"),
                dh("B = (" + type + ") (k * 11)"),
                dh("C = A + B"));
    }


    void testNumericalType(Table t, String columnA, String columnB, String columnSum) {
        t = t.where("i % 2  == 0");
        Table sumT = t.update("__testSum__ = " + columnA + " + " + columnB);
        assertEquals(sumT.where("(__testSum__) ==  " + columnSum).size(), t.size());
        sumT = t.update("__testSum__ = " + columnSum + " - " + columnB);
        assertEquals(sumT.where("(__testSum__) ==  " + columnA).size(), t.size());
        sumT = t.updateView("__testSum__ = " + columnSum + " - " + columnB);
        assertEquals(sumT.where("(__testSum__) ==  " + columnA).size(), t.size());

        sumT = t.update(nb("__testSum__ = " + columnA + " + " + columnB));
        assertEquals(sumT.where(nb("(__testSum__) ==  " + columnSum)).size(), t.size());
        sumT = t.update(nb("__testSum__ = " + columnSum + " - " + columnB));
        assertEquals(sumT.where(nb("(__testSum__) ==  " + columnA)).size(), t.size());
        sumT = t.updateView(nb("__testSum__ = " + columnSum + " - " + columnB));
        assertEquals(sumT.where(nb("(__testSum__) ==  " + columnA)).size(), t.size());
    }


    static Map<String, String> javaToPythonCasts = new HashMap<>();

    {
        javaToPythonCasts.put("byte", "int8");
        javaToPythonCasts.put("char", "uint16");
        javaToPythonCasts.put("short", "int16");
        javaToPythonCasts.put("int", "int32");
        javaToPythonCasts.put("long", "uint64");
        javaToPythonCasts.put("float", "float32");
        javaToPythonCasts.put("double", "float64");
    }

    @Test
    public void testNumericalTypes() {
        String[] types = {"byte", "char", "short", "int", "long", "float", "double"};
        PyObject.executeCode("import numpy as np\n", PyInputMode.SCRIPT);
        int[] sizes = {0, 1, 11, 100};
        for (int size : sizes) {
            for (String type : types) {
                testNumericalType(getSumTable(size, type), "A", "B", "C");
            }
        }
    }

    private static Table getBoolTable(int size) {
        return TableTools.emptyTable(size).select(dh("A = ((k%3) == 0)"), dh("B = ((k%2) == 0)"));
    }


    @Test
    public void testOneValue() {
        TstUtils.assertTableEquals(TableTools.emptyTable(1).update("Testv = 1"), TableTools.emptyTable(1).update(dh("Testv = 1")));
    }

    @Test
    public void testScope() {
        PyObject.executeCode("foo = 1", PyInputMode.STATEMENT);
        //Check that the evaluation completes at all
        TableTools.emptyTable(1).update("Testv = foo + 1");
        //... and then does it again to confirm it does the right thing
        TstUtils.assertTableEquals(TableTools.emptyTable(1).update("Testv = foo + 1"), TableTools.emptyTable(1).update(dh("Testv =(long) 2")));

        PyObject.executeCode("def testFoo2(incoming):\n" +
                "    foo = 11\n" +
                "    return incoming.update('Testv = foo + 1')\n", PyInputMode.SCRIPT);
        PyLib.getMainGlobals().asDict().setItem("emptyOne", TableTools.emptyTable(1));
        PyObject pyObject = PyObject.executeCode("testFoo2(emptyOne)", PyInputMode.EXPRESSION);
        TstUtils.assertTableEquals(
                pyObject.createProxy(Table.class),
                TableTools.emptyTable(1).update(dh("Testv =(long) 12")));

        TstUtils.assertTableEquals(TableTools.emptyTable(1).update("Testv = foo + 1"), TableTools.emptyTable(1).update(dh("Testv = (long)2")));

        PyObject.executeCode("def testFoo3(incoming,foo):\n" +
                "    return incoming.update('Testv = foo + 1')\n", PyInputMode.SCRIPT);
        pyObject = PyObject.executeCode("testFoo3(emptyOne,10)", PyInputMode.EXPRESSION);
        TstUtils.assertTableEquals(
                pyObject.createProxy(Table.class),
                TableTools.emptyTable(1).update(dh("Testv = (long)11")));

    }

    @Test
    public void testBoolean() {
        int sizes[] = {0, 1, 11, 100};
        for (int size : sizes) {
            Table t = getBoolTable(size);
            Table allTs = t.update("Odds = not B", "Even = True & B", "BySix = B and A", "By8 = (k % 8) == 0", "TrivialT = True", "TrivialF = False");
            FormulaParserConfiguration setParser = FormulaParserConfiguration.parser;
            TstUtils.assertTableEquals(allTs.where("Odds").view("A", "B"), t.where("k % 2 == 1"));
            TstUtils.assertTableEquals(allTs.where("Even").view("A", "B"), t.where("k % 2 == 0"));
            TstUtils.assertTableEquals(allTs.where("BySix").view("A", "B"), t.where("k % 6 == 0"));
            TstUtils.assertTableEquals(allTs.where("By8").view("A", "B"), t.where("k % 8  == 0"));
            TstUtils.assertTableEquals(allTs.where("Odds").view("A", "B"), t.where(dh("k % 2 == 1")));
            TstUtils.assertTableEquals(allTs.where("Even").view("A", "B"), t.where(dh("k % 2 == 0")));
            TstUtils.assertTableEquals(allTs.where("BySix").view("A", "B"), t.where(dh("k % 6 == 0")));
            TstUtils.assertTableEquals(allTs.where("By8").view("A", "B"), t.where(dh("k % 8  == 0")));
            TstUtils.assertTableEquals(allTs.where("TrivialT").view("A", "B"), t.where(dh("true")));
            TstUtils.assertTableEquals(allTs.where("TrivialF").view("A", "B"), t.where(dh("false")));
        }
    }

    private static Table getStringTable(int size) {
        return TableTools.emptyTable(size).select(dh("A = `` + (k * 11)"));
    }


    @Test
    public void testString() {
        if (NumbaCompileTools.pythonVersion.startsWith("2.")) {
            return;
        }
        int sizes[] = {0, 1, 11, 100};
        for (int size : sizes) {
            Table t = getStringTable(size);
            Table allTs = t.update("B = A + 'works'", "C = B[1]=='5'");
            TstUtils.assertTableEquals(allTs, t.update(dh("B = A + `works`"), dh("C = B.charAt(1)=='5'")));
            Table tt = allTs.where("B[1]=='5'");
            TstUtils.assertTableEquals(tt, allTs.where(dh("B.charAt(1)=='5'")));
        }

        for (int size : sizes) {
            Table t = TableTools.emptyTable(size);
            t = t.select("A = 'something '");
            Table allTs = t.update("B = A + 'works'", "C = B[1]=='5'");
            TstUtils.assertTableEquals(allTs, t.update(dh("B = A + `works`"), dh("C = B.charAt(1)=='5'")));
            Table tt = allTs.where("B[1]=='5'");
            TstUtils.assertTableEquals(tt, allTs.where(dh("B.charAt(1)=='5'")));
        }

        PyObject.executeCode("foo = 'something '", PyInputMode.STATEMENT);
        for (int size : sizes) {
            Table t = TableTools.emptyTable(size);
            t = t.select("A = foo + foo");
            Table allTs = t.update("B = A + 'works'", "C = B[1]=='5'");
            TstUtils.assertTableEquals(allTs, t.update(dh("B = A + `works`"), dh("C = B.charAt(1)=='5'")));
            Table tt = allTs.where("B[1]=='5'");
            TstUtils.assertTableEquals(tt, allTs.where(dh("B.charAt(1)=='5'")));
        }

    }

    private static Table getArrayTable(int size) {
        QueryScope.getDefaultInstance().putParam("size", size);
        return TableTools.emptyTable(size).select(dh("A7 = k%7"), dh("A13 = k%13"), dh("halfSize = k % size"));
    }

    @Test
    public void testUint32Type() {
        Table t = TableTools.emptyTable(1);
        PyObject.executeCode("import numpy", PyInputMode.SCRIPT);
        PyObject.executeCode("my32Int = numpy.uint32(15)", PyInputMode.SCRIPT);
        Table tn = t.update(nb("newCol=my32Int"));
        TstUtils.assertTableEquals(tn, t.update(dh("newCol = (long)15")));
    }


    @Test
    public void testArraysEtc() {
        PyObject.executeCode("import numpy as foo_np", PyInputMode.STATEMENT);
        PyObject.executeCode("mult = 10", PyInputMode.STATEMENT);
        int sizes[] = {0, 1, 11, 100};
        for (int size : sizes) {
            QueryScope.getDefaultInstance().putParam("size", size);
            Table source = TableTools.emptyTable(size).select(dh("A7 = k%7"), dh("A13 = k%13"), dh("halfSize = k % (size/2)"));
            String aggregationCriteria[] = {"x = i", "x = k % 5", "x = 1", "x = k % 10"};
            for (String aggregationCriterion : aggregationCriteria) {
                Table referenceTable = source.by(aggregationCriterion);
                Table inputs[] = {
                        referenceTable,
                        referenceTable.update(dh("A7 = A7.toArray()"), dh("A13 = A13.toArray()"), dh("halfSize = halfSize.toArray()"))
                };
                Table expected = referenceTable.update(dh("med13 = avg(A13)"), dh("avgAll = sum(halfSize)"), dh("prod13 = A13*10"), dh("first = A13.subArray(0,1)"));
                Table expectedResults[] = {
                        expected.update(dh("first = first.toArray()")),
                        expected.update(dh("A7 = A7.toArray()"), dh("A13 = A13.toArray()"), dh("halfSize = halfSize.toArray()"),dh("first = first.toArray()"))
                };
                for (int i = 0; i < inputs.length; i++) {
                    Table input = inputs[i];

                    //TableTools.show(input.update("med13 = foo_np.mean(A13)", "avgAll = foo_np.sum(halfSize)", "prod13 = A13*mult"));
                    TstUtils.assertTableEquals(input.update("med13 = foo_np.mean(A13)", "avgAll = foo_np.sum(halfSize)", "prod13 = A13*mult", "first = A13[0:1]"),
                            expectedResults[i]);
                }
            }
        }
    }

    private static class TypeThatDoesNotExistInPython { }

    /**
     * IDS-6063
     */
    @Test
    public void testNumbaFormulaNotInfluencedByUnusedColumns() {
        QueryScope.getDefaultInstance().putParam("java_only_object", new TypeThatDoesNotExistInPython());

        Table expected = TableTools.emptyTable(1)
            .update(dh("JavaOnly=java_only_object"))
            .update(dh("Foo=`bar`"));

        Table actual = TableTools.emptyTable(1)
            .update(dh("JavaOnly=java_only_object"))
            .update(nb("Foo='bar'"));

        TstUtils.assertTableEquals(expected, actual);
    }
}
