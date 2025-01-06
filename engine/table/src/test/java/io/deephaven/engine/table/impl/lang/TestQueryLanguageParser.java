//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

import groovy.lang.Closure;
import io.deephaven.base.Pair;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser.QueryLanguageParseException;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.PropertySaver;
import io.deephaven.engine.util.PyCallableWrapper;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import io.deephaven.vector.*;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;
import org.junit.Before;

import java.awt.*;
import java.time.Instant;
import java.util.List;
import java.util.*;

import static io.deephaven.engine.table.ColumnDefinition.ColumnType;
import static io.deephaven.engine.table.impl.lang.QueryLanguageParser.isWideningPrimitiveConversion;

@SuppressWarnings("InstantiatingObjectToGetClassObject")
public class TestQueryLanguageParser extends BaseArrayTestCase {

    private HashSet<Package> packageImports;
    private HashSet<Class<?>> classImports;
    private HashSet<Class<?>> staticImports;

    private HashMap<String, Class<?>> variables;
    private HashMap<String, Class<?>[]> variableParameterizedTypes;

    @Before
    @Override
    public void setUp() throws Exception {
        packageImports = new HashSet<>();
        packageImports.add(Package.getPackage("java.lang"));

        // Package.getPackage returns null if the class loader has yet to see a class from that package; force a load
        Package tablePackage = Table.class.getPackage();
        Assert.equals(tablePackage.getName(), "tablePackage.getName()", "io.deephaven.engine.table");
        packageImports.add(tablePackage);

        classImports = new HashSet<>();
        classImports.add(Color.class);
        classImports.add(VectorConversions.class);
        classImports.add(HashSet.class);
        classImports.add(HashMap.class);
        classImports.add(Vector.class);
        classImports.add(LanguageParserDummyClass.class);
        classImports.add(LanguageParserDummyClass.SubclassOfLanguageParserDummyClass.class);
        classImports.add(LanguageParserDummyEnum.class);
        classImports.add(LanguageParserDummyInterface.class);

        staticImports = new HashSet<>();
        staticImports.add(io.deephaven.function.Basic.class);
        staticImports.add(QueryLanguageFunctionUtils.class);
        staticImports.add(Math.class);
        staticImports.add(QueryConstants.class);
        staticImports.add(TestQueryLanguageParser.class);
        staticImports.add(LanguageParserDummyClass.StaticNestedClass.class);

        variables = new HashMap<>();
        variables.put("myByte", byte.class);
        variables.put("myShort", short.class);
        variables.put("myChar", char.class);
        variables.put("myInt", int.class);
        variables.put("myOtherInt", int.class);
        variables.put("myLong", long.class);
        variables.put("myOtherLong", long.class);
        variables.put("myFloat", float.class);
        variables.put("myDouble", double.class);
        variables.put("myBoolean", boolean.class);
        variables.put("myString", String.class);
        variables.put("myCharSequence", CharSequence.class);
        variables.put("myObject", Object.class);
        variables.put("myTestClass", TestClass.class);

        variables.put("myCharArray", new char[0].getClass());
        variables.put("myByteArray", new byte[0].getClass());
        variables.put("myIntArray", new int[0].getClass());
        variables.put("myLongArray", new long[0].getClass());
        variables.put("myDoubleArray", new double[0].getClass());

        variables.put("myTestClassArray", new TestClass[0].getClass());
        variables.put("myDoubleObjArray", new Double[0].getClass());
        variables.put("myIntegerObjArray", new Integer[0].getClass());

        variables.put("myByteObj", Byte.class);
        variables.put("myShortObj", Short.class);
        variables.put("myCharObj", Character.class);
        variables.put("myIntObj", Integer.class);
        variables.put("myLongObj", Long.class);
        variables.put("myFloatObj", Float.class);
        variables.put("myDoubleObj", Double.class);
        variables.put("myBooleanObj", Boolean.class);

        variables.put("myArrayList", ArrayList.class);
        variables.put("myParameterizedArrayList", ArrayList.class);
        variables.put("myHashMap", HashMap.class);
        variables.put("myParameterizedHashMap", HashMap.class);
        variables.put("myParameterizedClass", LanguageParserDummyClass.StaticNestedGenericClass.class);
        variables.put("myArrayParameterizedClass", LanguageParserDummyClass.StaticNestedGenericClass.class);

        variables.put("myVector", ObjectVector.class);
        variables.put("myCharVector", CharVector.class);
        variables.put("myByteVector", ByteVector.class);
        variables.put("myShortVector", ShortVector.class);
        variables.put("myIntVector", IntVector.class);
        variables.put("myFloatVector", FloatVector.class);
        variables.put("myLongVector", LongVector.class);
        variables.put("myDoubleVector", DoubleVector.class);
        variables.put("myObjectVector", ObjectVector.class);

        variables.put("myDummyClass", LanguageParserDummyClass.class);
        variables.put("myDummyInnerClass", LanguageParserDummyClass.InnerClass.class);
        variables.put("myDummyStaticNestedClass", LanguageParserDummyClass.StaticNestedClass.class);
        variables.put("myClosure", Closure.class);
        variables.put("myInstant", Instant.class);
        variables.put("myEnumValue", LanguageParserDummyEnum.class);

        variables.put("myTable", Table.class);
        variables.put("myPyObject", PyObject.class);
        variables.put("myPyCallable", PyCallableWrapper.class);

        variables.put("ExampleQuantity", int.class);
        variables.put("ExampleQuantity2", double.class);
        variables.put("ExampleQuantity3", double.class);
        variables.put("ExampleQuantity4", double.class);
        variables.put("ExampleStr", String.class);

        variables.put("genericSub", TestGenericSub.class);

        variableParameterizedTypes = new HashMap<>();
        variableParameterizedTypes.put("myParameterizedArrayList", new Class[] {Long.class});
        variableParameterizedTypes.put("myParameterizedHashMap", new Class[] {Integer.class, Double.class});
        variableParameterizedTypes.put("myVector", new Class[] {Double.class});

        variableParameterizedTypes.put("myParameterizedClass", new Class[] {String.class});
        variableParameterizedTypes.put("myArrayParameterizedClass", new Class[] {new String[0].getClass()});
    }

    public void testSimpleCalculations() throws Exception {
        String expression = "1+1";
        String resultExpression = "plus(1, 1)";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "1*1.0";
        resultExpression = "multiply(1, 1.0)";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1/1L";
        resultExpression = "divide(1, 1L)";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1%1L";
        resultExpression = "remainder(1, 1L)";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "1.0+2L+3*4";
        resultExpression = "plus(plus(1.0, 2L), multiply(3, 4))";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1==1";
        resultExpression = "eq(1, 1)";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "'g'=='g'";
        resultExpression = "eq('g', 'g')";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "false ^ true";
        resultExpression = "false ^ true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "false ^ null";
        resultExpression = "xor(false, null)";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "false | true";
        resultExpression = "false | true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "false | null";
        resultExpression = "binaryOr(false, null)";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "false & true";
        resultExpression = "false & true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "false & null";
        resultExpression = "binaryAnd(false, null)";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "1>1+2*3/4 || true";
        resultExpression = "greater(1, plus(1, divide(multiply(2, 3), 4))) || true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "1>1+2*3/4 && true";
        resultExpression = "greater(1, plus(1, divide(multiply(2, 3), 4))) && true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "(1>1+2*3/4) | true";
        resultExpression = "(greater(1, plus(1, divide(multiply(2, 3), 4)))) | true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "(1>1+2*3/4) & true";
        resultExpression = "(greater(1, plus(1, divide(multiply(2, 3), 4)))) & true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "1+\"test\"";
        resultExpression = "1 + \"test\"";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "1==1 ? 1 + 1 : 1.0";
        resultExpression = "eq(1, 1) ? plus(1, 1) : 1.0";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "(double) 1";
        resultExpression = "doubleCast(1)";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "(double[]) myObject";
        resultExpression = "(double[]) myObject";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myObject"});

        expression = "(double[][]) myObject";
        resultExpression = "(double[][]) myObject";
        check(expression, resultExpression, new double[0][0].getClass(), new String[] {"myObject"});

        expression = "1<2";
        resultExpression = "less(1, 2)";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "1>2";
        resultExpression = "greater(1, 2)";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "1<=2";
        resultExpression = "lessEquals(1, 2)";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "1>=2";
        resultExpression = "greaterEquals(1, 2)";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "-1";
        resultExpression = "negate(1)";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "!false";
        resultExpression = "!false";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "!Boolean.FALSE";
        resultExpression = "not(Boolean.FALSE)";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "myInt * myLong + myOtherInt * myOtherLong + myLongVector[myInt - 1]";
        resultExpression =
                "plus(plus(multiply(myInt, myLong), multiply(myOtherInt, myOtherLong)), myLongVector.get(longCast(minus(myInt, 1))))";
        check(expression, resultExpression, long.class,
                new String[] {"myInt", "myLong", "myOtherInt", "myOtherLong", "myLongVector"});
    }

    /**
     * Test literal ints and longs
     */
    public void testIntegralLiterals() throws Exception {
        String expression = "42";
        String resultExpression = "42";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "4_2";
        resultExpression = "4_2";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "4_2L";
        resultExpression = "4_2L";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "1_000_000_000";
        resultExpression = "1_000_000_000";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "1_000_000_000L";
        resultExpression = "1_000_000_000L";
        check(expression, resultExpression, long.class, new String[] {});
    }

    /**
     * Test literal floats and doubles
     */
    public void testDecimalLiterals() throws Exception {
        String expression = "42d";
        String resultExpression = "42d";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "42f";
        resultExpression = "42f";
        check(expression, resultExpression, float.class, new String[] {});

        expression = "1.0";
        resultExpression = "1.0";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1.0d";
        resultExpression = "1.0d";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1.0f";
        resultExpression = "1.0f";
        check(expression, resultExpression, float.class, new String[] {});

        expression = "1_000_000_000d";
        resultExpression = "1_000_000_000d";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1_000_000_000f";
        resultExpression = "1_000_000_000f";
        check(expression, resultExpression, float.class, new String[] {});

        expression = "1_000_000_000.0";
        resultExpression = "1_000_000_000.0";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1_000_000_000.0f";
        resultExpression = "1_000_000_000.0f";
        check(expression, resultExpression, float.class, new String[] {});

    }

    /**
     * Test hexadecimal literals (ints and longs)
     */
    public void testHexadecimalLiterals() throws Exception {
        String expression = "0x00";
        String resultExpression = "0x00";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "0x00L";
        resultExpression = "0x00L";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "0x44332211";
        resultExpression = "0x44332211";
        check(expression, resultExpression, int.class, new String[] {});
    }

    /**
     * Test binary literals (ints and longs)
     */
    public void testBinaryLiterals() throws Exception {
        String expression = "0b0";
        String resultExpression = "0b0";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "0b00000000";
        resultExpression = "0b00000000";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "0b00000000L";
        resultExpression = "0b00000000L";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "0b1111111000000001111111100000000"; // 35 bits -- int
        resultExpression = "0b1111111000000001111111100000000";
        check(expression, resultExpression, int.class, new String[] {});

        // Note that the query language automatically converts to a long (whereas java would not)
        expression = "0b1111111100000000111111110000000011111111L";
        resultExpression = "0b1111111100000000111111110000000011111111L";
        check(expression, resultExpression, long.class, new String[] {});
    }

    /**
     * The query language will automatically convert an integer literal to a long literal if the value is too big to
     * store as an int. (Normal Java would not do this; it would just result in a compilation error.)
     */
    public void testAutoPromotedLiterals() throws Exception {
        String expression, resultExpression;

        expression = "1_000_000_000_000";
        resultExpression = "1_000_000_000_000L";
        check(expression, resultExpression, long.class, new String[] {});

        // 5 bytes (one too many)
        expression = "0x5544332211";
        resultExpression = "0x5544332211L";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "0b11111111000000001111111100000000"; // 32 bits -- must be a long
        resultExpression = "0b11111111000000001111111100000000L";
        check(expression, resultExpression, long.class, new String[] {});
    }

    public void testBooleanLiterals() throws Exception {
        String expression = "true";
        String resultExpression = "true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "false";
        resultExpression = "false";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "Boolean.TRUE";
        resultExpression = "Boolean.TRUE";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "Boolean.FALSE";
        resultExpression = "Boolean.FALSE";
        check(expression, resultExpression, Boolean.class, new String[] {});
    }

    public void testCharLiterals() throws Exception {
        String expression = "'c'";
        String resultExpression = "'c'";
        check(expression, resultExpression, char.class, new String[] {});

        expression = "'='";
        resultExpression = "'='";
        check(expression, resultExpression, char.class, new String[] {});

        expression = "'`'";
        resultExpression = "'`'";
        check(expression, resultExpression, char.class, new String[] {});

        expression = "'\"'";
        resultExpression = "'\"'";
        check(expression, resultExpression, char.class, new String[] {});

        // Test a crazy unicode character. (This is an arrow - ↳ - U+21b3)
        expression = "'\\u21b3'";
        resultExpression = "'\\u21b3'";
        check(expression, resultExpression, char.class, new String[] {});
    }

    public void testConvertBackticks() {
        Require.equals(
                QueryLanguageParser.convertBackticks("`hello`"),
                "convertBackticks(\"`hello`\")",
                "\"hello\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("`'`"),
                "convertBackticks(\"`'`\")",
                "\"'\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("`\"`"),
                "convertBackticks(\"`\\\"`\")",
                "\"\\\"\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("\"`\""),
                "convertBackticks(\"\\\"`\\\"\")",
                "\"`\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("`'\\\"'`"),
                "convertBackticks(\"`'\\\\\\\"'`\")",
                "\"'\\\"'\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("\"`abc`\""),
                "convertBackticks(\"\\\"`abc`\\\"\")",
                "\"`abc`\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("\"`'abc`'\""),
                "convertBackticks(\"\\\"`'abc`'\\\"\")",
                "\"`'abc`'\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("\"'`\""),
                "convertBackticks(\"\\\"'`\\\"\")",
                "\"'`\"");

        Require.equals(
                QueryLanguageParser.convertBackticks("`abc ` + \"def\" + \"`hij`\" + '`' + `'`"),
                "convertBackticks(\"`abc ` + \\\"def\\\" + \\\"`hij`\\\" + '`' + `'`\")",
                "\"abc \" + \"def\" + \"`hij`\" + '`' + \"'\"");

        // test each type of quote, escaped and contained within itself
        Require.equals(
                QueryLanguageParser.convertBackticks("\"\\\"\""),
                "convertBackticks(\"\\\"\\\\\\\"\\\"\")",
                "\"\\\"\"");
        Require.equals(
                QueryLanguageParser.convertBackticks("`\\``"),
                "convertBackticks(\"`\\\\``\")",
                "\"\\`\"");
        Require.equals(
                QueryLanguageParser.convertBackticks("'\\''"),
                "convertBackticks(\"'\\\\''\")",
                "'\\''");

        // test tick and double quote both escaped within a string
        Require.equals(
                QueryLanguageParser.convertBackticks("`\"\\``"),
                "convertBackticks(\"`\\\"\\\\``\")",
                "\"\\\"\\`\"");
        // here ` is unescaped, since it is within "s
        Require.equals(
                QueryLanguageParser.convertBackticks("\"\\\"`\""),
                "convertBackticks(\"\\\"\\\\\\\"`\\\"\")",
                "\"\\\"`\"");

        // confirm that standard java escaping tools are sufficient to correctly escape strings for the LangParser
        Require.equals(
                QueryLanguageParser.convertBackticks("\"" + StringEscapeUtils.escapeJava("`\"'\\") + "\""),
                "convertBackticks(escapeJava(\"`\\\"'\\\\\"))",
                "\"`\\\"'\\\\\"");
    }

    public void testConvertSingleEquals() {
        Require.equals(
                QueryLanguageParser.convertSingleEquals("a=b"),
                "convertSingleEquals(\"a=b\")",
                "a==b");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("a=b=c==d=e==f"),
                "convertSingleEquals(\"a=b=c==d=e==f\")",
                "a==b==c==d==e==f");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("'='"),
                "convertSingleEquals(\"'='\")",
                "'='");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("'='='='"),
                "convertSingleEquals(\"'='='='\")",
                "'='=='='");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("'='='='=='='"),
                "convertSingleEquals(\"'='='='=='='\")",
                "'='=='='=='='");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("a='='=b"),
                "convertSingleEquals(\"a='='=b\")",
                "a=='='==b");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("\"a=b\""),
                "convertSingleEquals(\"a=b\")",
                "\"a=b\"");

        Require.equals(
                QueryLanguageParser.convertSingleEquals("\"a=b'\"='='"),
                "convertSingleEquals(\"\\\"a=b'\\\"='='\")",
                "\"a=b'\"=='='");
    }

    /**
     * Test casts. See <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.5">Chapter 5,
     * Conversions and Contexts</a>, in the java language specification for more info.
     *
     * @see #testPrimitiveLiteralCasts()
     * @see #testBoxedToPrimitiveCasts()
     */
    public void testMiscellaneousCasts() throws Exception {
        String expression;
        String resultExpression;

        // Test casting to object types:
        expression = "(CharSequence)myString";
        resultExpression = "(CharSequence) myString";
        check(expression, resultExpression, CharSequence.class, new String[] {"myString"});

        expression = "(String)(CharSequence)myString";
        resultExpression = "(String) ((CharSequence) myString)";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "(Double)myDouble";
        resultExpression = "(Double) myDouble";
        check(expression, resultExpression, Double.class, new String[] {"myDouble"});

        expression = "(Double)(double)myInt";
        resultExpression = "(Double) (doubleCast(myInt))";
        check(expression, resultExpression, Double.class, new String[] {"myInt"});

        expression = "(double)(int)myIntObj";
        resultExpression = "doubleCast(intCast(myIntObj))";
        check(expression, resultExpression, double.class, new String[] {"myIntObj"});

        expression = "(double)myIntObj"; // requires separate casts for unboxing & widening (see notes at
                                         // testBoxedToPrimitiveCasts, or JLS)
        resultExpression = "doubleCast(intCast(myIntObj))";
        check(expression, resultExpression, double.class, new String[] {"myIntObj"});

        expression = "(short)(double)(Double)(double)myInt";
        resultExpression = "shortCast(doubleCast((Double) (doubleCast(myInt))))";
        check(expression, resultExpression, short.class, new String[] {"myInt"});

        // TOOD: Test some invalid casts?

        try {
            // Test invalid boxing
            expression = "(Double)myInt";
            resultExpression = "(Double) myInt";
            check(expression, resultExpression, Double.class, new String[] {"myInt"});
            fail("Should have throw a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }
    }

    /**
     * This is an older version of {@link #testPrimitiveVariableCasts()}, operating with literals instead of variables.
     */
    public void testPrimitiveLiteralCasts() throws Exception {
        String expression, resultExpression;

        Collection<Pair<String, Class<?>>> literals = Arrays.asList(
                new Pair<>("42", int.class),
                new Pair<>("42L", long.class),
                new Pair<>("42f", float.class),
                new Pair<>("42d", double.class),
                new Pair<>("'c'", char.class));
        Collection<Pair<String, Class<?>>> targetTypes = Arrays.asList(
                new Pair<>("char", char.class),
                new Pair<>("byte", byte.class),
                new Pair<>("short", short.class),
                new Pair<>("int", int.class),
                new Pair<>("float", float.class),
                new Pair<>("double", double.class),
                new Pair<>("long", long.class));

        /*
         * Test casting from each possible numeric literal type (and char) to each of the other numeric types (and
         * char).
         *
         * When casting to a primitive type, we replace the cast with a function call (e.g "(int)foo" to "intCast(foo)")
         *
         * The exception is the identity conversion, e.g. "(int)42" or "(double)42d". Since mid-2017, there is are no
         * intermediate functions for these redundant conversions.
         */
        for (Pair<String, Class<?>> literal : literals) {
            for (Pair<String, Class<?>> targetType : targetTypes) {
                expression = '(' + targetType.first + ") " + literal.first; // e.g. "(int)42"
                if (targetType.second == literal.second) {
                    resultExpression = expression;
                } else {
                    resultExpression = targetType.first + "Cast(" + literal.first + ')'; // e.g. "intCast(42)"
                }
                check(expression, resultExpression, targetType.second, new String[] {});
            }
        }

        // Test casting booleans types to numeric types (which should fail)
        for (Pair<String, Class<?>> targetType : targetTypes) {
            try {
                try {
                    expression = '(' + targetType.first + ")true"; // e.g. "(int)true"
                    resultExpression = targetType.first + "Cast(true)"; // e.g. "intCast(true)"
                    check(expression, resultExpression, targetType.second, new String[] {});
                    fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
                } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed testing cast of boolean to " + targetType.second.getName(), ex);
            }
        }

        // Test casting numeric types to booleans (which should fail)
        for (Pair<String, Class<?>> literal : literals) {
            try {
                try {
                    resultExpression = expression = "(boolean)" + literal.first; // e.g. "(boolean)42"
                    check(expression, resultExpression, boolean.class, new String[] {});
                    fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
                } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed testing cast of " + literal.second.getName() + " to boolean", ex);
            }
        }

        // Test the identity conversion for booleans
        expression = resultExpression = "(boolean) true"; // e.g. "(int)true"
        check(expression, resultExpression, boolean.class, new String[] {});
    }

    /**
     * Test conversions between various primitive types. This is a newer and more complete version of
     * {@link #testPrimitiveLiteralCasts()}. (This one handles {@code byte} and {@code short}.)
     * <p>
     * See table 5.5-A <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.5">here</a>.
     *
     * @see #testBoxedToPrimitiveCasts()
     */
    public void testPrimitiveVariableCasts() throws Exception {
        String expression, resultExpression;

        Collection<Pair<String, Class<?>>> numericAndCharVars = Arrays.asList(
                new Pair<>("myByte", byte.class),
                new Pair<>("myShort", short.class),
                new Pair<>("myChar", char.class),
                new Pair<>("myInt", int.class),
                new Pair<>("myLong", long.class),
                new Pair<>("myFloat", float.class),
                new Pair<>("myDouble", double.class));
        Collection<Pair<String, Class<?>>> numericAndCharTypes = Arrays.asList(
                new Pair<>("byte", byte.class),
                new Pair<>("short", short.class),
                new Pair<>("char", char.class),
                new Pair<>("int", int.class),
                new Pair<>("long", long.class),
                new Pair<>("float", float.class),
                new Pair<>("double", double.class));

        /*
         * Test casting from each possible numeric literal type (and char) to each of the other numeric types (and
         * char).
         *
         * When casting to a primitive type, we replace the cast with a function call (e.g "(int)foo" to "intCast(foo)")
         *
         * The exception is the identity conversion, e.g. "(int)myInt" or "(double)myDouble". Since mid-2017, there is
         * are no intermediate functions for these redundant conversions.
         */
        for (Pair<String, Class<?>> var : numericAndCharVars) {
            for (Pair<String, Class<?>> targetType : numericAndCharTypes) {
                expression = '(' + targetType.first + ") " + var.first; // e.g. "(int)myDouble"
                if (targetType.second == var.second) {
                    resultExpression = expression;
                } else {
                    resultExpression = targetType.first + "Cast(" + var.first + ')'; // e.g. "intCast(myDouble)"
                }
                check(expression, resultExpression, targetType.second, new String[] {var.first});
            }
        }

        // Test casting booleans types to numeric/char types (which should fail)
        for (Pair<String, Class<?>> targetType : numericAndCharTypes) {
            try {
                try {
                    expression = '(' + targetType.first + ")myBoolean"; // e.g. "(int)myBoolean"
                    resultExpression = targetType.first + "Cast(myBoolean)"; // e.g. "intCast(myBoolean)"
                    check(expression, resultExpression, targetType.second, new String[] {"myBoolean"});
                    fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
                } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed testing cast of boolean to " + targetType.second.getName(), ex);
            }
        }

        // Test casting numeric/char types to booleans (which should fail)
        for (Pair<String, Class<?>> var : numericAndCharVars) {
            try {
                try {
                    resultExpression = expression = "(boolean)" + var.first; // e.g. "(boolean)myInt"
                    check(expression, resultExpression, boolean.class, new String[] {});
                    fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
                } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed testing cast of " + var.second.getName() + " to boolean", ex);
            }
        }

        // Test the identity conversion for booleans
        expression = resultExpression = "(boolean) true"; // e.g. "(int)true"
        check(expression, resultExpression, boolean.class, new String[] {});

    }

    /**
     * Test conversions from boxed types to primitive types.
     * <p>
     * When casting from boxed types to primitive types, the target type must be at least as wide as the primitive type
     * that the source (boxed) type represents. Thus there are two kinds of conversions from boxed types to primitive
     * types:
     * <p>
     * 1. Unboxing-only conversions (e.g. Integer to int) 2. Unboxing and widening conversions (e.g. Integer to double)
     * <p>
     * In the latter case, the language must explicitly cast an Integer to an int *before* casting it to a double. This
     * follow's the language specification: when permitted, a non-identity conversion from a boxed type to a primitive
     * type consists of an unboxing conversion followed by a widening conversion.
     * <p>
     * For example, this code: {@code (double) new Integer(42) } Should be parsed into:
     * {@code doubleCast(intCast(new Integer(42))) }
     * <p>
     * Otherwise, the compiler would see {@code doubleCast()} with an {@code Integer} argument, and rightly decide that
     * {@code doubleCast(Object)} is a better choice than {@code doubleCast(int)}. But then we wind up running:
     * {@code (double) anObject} when 'anObject' is actually an Integer. Java then tries a narrowing conversion from
     * {@code Object} to {@code Double}, which fails.
     * <p>
     * See table 5.5-A <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.5">here</a>.
     *
     * @see #testPrimitiveLiteralCasts()
     * @see #testBoxedToPrimitiveCasts()
     */
    public void testBoxedToPrimitiveCasts() {
        String expression, resultExpression;

        final List<Class<?>> boxedTypes = new ArrayList<>(io.deephaven.util.type.TypeUtils.BOXED_TYPES);
        final List<Class<?>> primitiveTypes = new ArrayList<>(io.deephaven.util.type.TypeUtils.PRIMITIVE_TYPES);

        for (int i = 0; i < io.deephaven.util.type.TypeUtils.BOXED_TYPES.size(); i++) {
            final Class<?> boxedType = boxedTypes.get(i);
            // the name of the primitive type that this boxed type represents
            final String unboxedTypeName = io.deephaven.util.type.TypeUtils.getUnboxedType(boxedType).getName();
            final String boxedTypeTestVarName =
                    "my" + Character.toUpperCase(unboxedTypeName.charAt(0)) + unboxedTypeName.substring(1) + "Obj";

            for (int j = 0; j < io.deephaven.util.type.TypeUtils.PRIMITIVE_TYPES.size(); j++) {
                final Class<?> primitiveType = primitiveTypes.get(j);
                final String primitiveTypeName = primitiveType.getName();

                expression = '(' + primitiveTypeName + ") " + boxedTypeTestVarName;
                if (primitiveType == boolean.class) {
                    resultExpression = expression; // There is no "booleanCast()" function
                } else if (i == j) { // Unboxing conversion only
                    resultExpression = primitiveTypeName + "Cast(" + boxedTypeTestVarName + ')';
                } else { // i != j; Unboxing and widening conversion
                    resultExpression =
                            primitiveTypeName + "Cast(" + unboxedTypeName + "Cast(" + boxedTypeTestVarName + "))";
                }

                try {
                    if (j < i
                            || (primitiveType == Character.TYPE && boxedType != Character.class)
                            || (boxedType == Boolean.class ^ primitiveType == Boolean.TYPE)) {
                        /*
                         * Ensure we fail on conversions that the JLS disallows. Such as: 1) Trying to convert to a
                         * primitive type that is not sufficient to store the boxed type's data (i.e. j < i). The JLS
                         * does not permit such a conversion. 2) Trying to unbox anything other than a Character to a
                         * char. (However, char can be cast to wider (int, long, float, double). 3) Casting a Boolean to
                         * any primitive besides bool, or casting anything besides a Boolean to bool.
                         *
                         * Note that casting from less-specific types to any primitive is supported.
                         */
                        try {
                            check(expression, resultExpression, primitiveType, new String[] {boxedTypeTestVarName});
                            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
                        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
                        }
                    } else {
                        check(expression, resultExpression, primitiveType, new String[] {boxedTypeTestVarName});
                    }
                } catch (Throwable ex) {
                    throw new RuntimeException("Failed testing cast of " + boxedType.getName() + " to "
                            + primitiveType.getName() + " (i=" + i + ", j=" + j + ')', ex);
                }
            }
        }
    }

    /**
     * Test casting an Object to all primitive types and all boxed types.
     * <p>
     * This should be a narrowing and unboxing conversion.
     */
    public void testObjectToPrimitiveOrBoxedCasts() throws Exception {
        String expression, resultExpression;

        Set<Class<?>> boxedAndPrimitiveTypes = new HashSet<>();
        boxedAndPrimitiveTypes.addAll(io.deephaven.util.type.TypeUtils.BOXED_TYPES);
        boxedAndPrimitiveTypes.addAll(io.deephaven.util.type.TypeUtils.PRIMITIVE_TYPES);

        for (Class<?> type : boxedAndPrimitiveTypes) {
            expression = '(' + type.getSimpleName() + ") myObject";
            if (type.isPrimitive() && type != boolean.class) {
                resultExpression = io.deephaven.util.type.TypeUtils.getUnboxedType(type) + "Cast(myObject)";
            } else {
                resultExpression = expression;
            }
            check(expression, resultExpression, type, new String[] {"myObject"});
        }
    }

    /**
     * Test casting all primitive types and all boxed types to Object. (Can't hurt.)
     */
    public void testPrimitiveAndBoxedToObjectCasts() throws Exception {
        String expression, resultExpression;

        Set<Class<?>> boxedAndPrimitiveTypes = new HashSet<>();
        boxedAndPrimitiveTypes.addAll(io.deephaven.util.type.TypeUtils.BOXED_TYPES);
        boxedAndPrimitiveTypes.addAll(io.deephaven.util.type.TypeUtils.PRIMITIVE_TYPES);

        for (Class<?> type : boxedAndPrimitiveTypes) {
            expression = '(' + type.getSimpleName() + ") myObject";
            if (type.isPrimitive() && type != boolean.class) {
                resultExpression = TypeUtils.getUnboxedType(type) + "Cast(myObject)";
            } else {
                resultExpression = expression;
            }
            check(expression, resultExpression, type, new String[] {"myObject"});
        }
    }

    public void testPyObjectToPrimitiveCasts() throws Exception {
        String expression = "(int)myPyObject";
        String resultExpression = "intPyCast(myPyObject)";
        check(expression, resultExpression, int.class, new String[] {"myPyObject"});

        expression = "(double)myPyObject";
        resultExpression = "doublePyCast(myPyObject)";
        check(expression, resultExpression, double.class, new String[] {"myPyObject"});

        expression = "(long)myPyObject";
        resultExpression = "longPyCast(myPyObject)";
        check(expression, resultExpression, long.class, new String[] {"myPyObject"});

        expression = "(float)myPyObject";
        resultExpression = "floatPyCast(myPyObject)";
        check(expression, resultExpression, float.class, new String[] {"myPyObject"});

        expression = "(char)myPyObject";
        resultExpression = "charPyCast(myPyObject)";
        check(expression, resultExpression, char.class, new String[] {"myPyObject"});

        expression = "(byte)myPyObject";
        resultExpression = "bytePyCast(myPyObject)";
        check(expression, resultExpression, byte.class, new String[] {"myPyObject"});

        expression = "(short)myPyObject";
        resultExpression = "shortPyCast(myPyObject)";
        check(expression, resultExpression, short.class, new String[] {"myPyObject"});

        expression = "(String)myPyObject";
        resultExpression = "doStringPyCast(myPyObject)";
        check(expression, resultExpression, String.class, new String[] {"myPyObject"});

        expression = "(boolean)myPyObject";
        resultExpression = "booleanPyCast(myPyObject)";
        check(expression, resultExpression, boolean.class, new String[] {"myPyObject"});

        expression = "(Boolean)myPyObject";
        resultExpression = "doBooleanPyCast(myPyObject)";
        check(expression, resultExpression, Boolean.class, new String[] {"myPyObject"});
    }

    public void testVariables() throws Exception {
        String expression = "1+myInt";
        String resultExpression = "plus(1, myInt)";
        check(expression, resultExpression, int.class, new String[] {"myInt"});

        expression = "1*myDouble";
        resultExpression = "multiply(1, myDouble)";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "myInt/myLong";
        resultExpression = "divide(myInt, myLong)";
        check(expression, resultExpression, double.class, new String[] {"myInt", "myLong"});

        expression = "myDouble+myLong+3*4";
        resultExpression = "plus(plus(myDouble, myLong), multiply(3, 4))";
        check(expression, resultExpression, double.class, new String[] {"myDouble", "myLong"});

        expression = "1==myInt";
        resultExpression = "eq(1, myInt)";
        check(expression, resultExpression, boolean.class, new String[] {"myInt"});

        expression = "myInt>1+2*myInt/4 || myBoolean";
        resultExpression = "greater(myInt, plus(1, divide(multiply(2, myInt), 4))) || myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean", "myInt"});

        expression = "myInt>1+2*myInt/4 | myBoolean";
        resultExpression = "greater(myInt, plus(1, divide(multiply(2, myInt), 4))) | myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean", "myInt"});

        expression = "myInt+myString";
        resultExpression = "myInt + myString";
        check(expression, resultExpression, String.class, new String[] {"myInt", "myString"});

        expression = "myTestClass";
        resultExpression = "myTestClass";
        check(expression, resultExpression, TestClass.class, new String[] {"myTestClass"});

        expression = "myIntArray[5]";
        resultExpression = "myIntArray[5]";
        check(expression, resultExpression, int.class, new String[] {"myIntArray"});

        expression = "-myInt";
        resultExpression = "negate(myInt)";
        check(expression, resultExpression, int.class, new String[] {"myInt"});

        expression = "!myBoolean";
        resultExpression = "!myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean"});

        expression = "!myBooleanObj";
        resultExpression = "not(myBooleanObj)";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanObj"});

        expression = "(String)myString==null";
        resultExpression = "isNull((String) myString)";
        check(expression, resultExpression, boolean.class, new String[] {"myString"});

        expression = "myDoubleArray.length";
        resultExpression = "myDoubleArray.length";
        check(expression, resultExpression, int.class, new String[] {"myDoubleArray"});
    }

    public void testConditionalExpressions() throws Exception {
        String expression = "1==1 ? myString : null";
        String resultExpression = "eq(1, 1) ? myString : null";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "1==1 ? null : myString";
        resultExpression = "eq(1, 1) ? null : myString";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "1==1 ? 10 : null";
        resultExpression = "eq(1, 1) ? 10 : NULL_INT";
        check(expression, resultExpression, int.class, new String[0]);

        expression = "1==1 ? null : 10.0";
        resultExpression = "eq(1, 1) ? NULL_DOUBLE : 10.0";
        check(expression, resultExpression, double.class, new String[0]);

        expression = "1==1 ? true : null";
        resultExpression = "eq(1, 1) ? (Boolean) true : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[0]);

        expression = "1==1 ? true && true : null";
        resultExpression = "eq(1, 1) ? (Boolean) (true && true) : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[0]);

        expression = "1==1 ? true && true && true : null";
        resultExpression = "eq(1, 1) ? (Boolean) (true && true && true) : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[0]);
    }

    public void testEqualsNull() throws Exception {
        String expression = "myString == null ? myString : null";
        String resultExpression = "isNull(myString) ? myString : null";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "null == myString ? myString : null";
        resultExpression = "isNull(myString) ? myString : null";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "myString == null ? myString : \"hello\"";
        resultExpression = "isNull(myString) ? myString : \"hello\"";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "myBoolean == null ? myBoolean : null";
        resultExpression = "isNull(myBoolean) ? (Boolean) myBoolean : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[] {"myBoolean"});

        expression = "myBoolean == null ? myBoolean : false";
        resultExpression = "isNull(myBoolean) ? myBoolean : false";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean"});

        expression = "myBooleanObj == null ? myBooleanObj : null";
        resultExpression = "isNull(myBooleanObj) ? myBooleanObj : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanObj"});

        expression = "myBooleanObj == null ? myBooleanObj : false";
        resultExpression = "isNull(myBooleanObj) ? myBooleanObj : (Boolean) false";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanObj"});

        expression = "myByte == null ? myByte : null";
        resultExpression = "isNull(myByte) ? myByte : NULL_BYTE";
        check(expression, resultExpression, byte.class, new String[] {"myByte"});

        expression = "myByte == null ? myByte : (byte) 1";
        resultExpression = "isNull(myByte) ? myByte : byteCast(1)";
        check(expression, resultExpression, byte.class, new String[] {"myByte"});

        expression = "myShort == null ? myShort : null";
        resultExpression = "isNull(myShort) ? myShort : NULL_SHORT";
        check(expression, resultExpression, short.class, new String[] {"myShort"});

        expression = "myShort == null ? myShort : (short) 12345";
        resultExpression = "isNull(myShort) ? myShort : shortCast(12345)";
        check(expression, resultExpression, short.class, new String[] {"myShort"});

        expression = "myInt == null ? myInt : null";
        resultExpression = "isNull(myInt) ? myInt : NULL_INT";
        check(expression, resultExpression, int.class, new String[] {"myInt"});

        expression = "myInt == null ? myInt : 12345";
        resultExpression = "isNull(myInt) ? myInt : 12345";
        check(expression, resultExpression, int.class, new String[] {"myInt"});

        expression = "myFloat == null ? myFloat : null";
        resultExpression = "isNull(myFloat) ? myFloat : NULL_FLOAT";
        check(expression, resultExpression, float.class, new String[] {"myFloat"});

        expression = "myFloat == null ? myFloat : 123.45f";
        resultExpression = "isNull(myFloat) ? myFloat : 123.45f";
        check(expression, resultExpression, float.class, new String[] {"myFloat"});

        expression = "myLong == null ? myLong : null";
        resultExpression = "isNull(myLong) ? myLong : NULL_LONG";
        check(expression, resultExpression, long.class, new String[] {"myLong"});

        expression = "myLong == null ? myLong : 12345L";
        resultExpression = "isNull(myLong) ? myLong : 12345L";
        check(expression, resultExpression, long.class, new String[] {"myLong"});

        expression = "myDouble == null ? myDouble : null";
        resultExpression = "isNull(myDouble) ? myDouble : NULL_DOUBLE";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "myDouble == null ? myDouble : 123.45d";
        resultExpression = "isNull(myDouble) ? myDouble : 123.45d";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});
    }

    public void testNotEqualsNull() throws Exception {
        String expression = "myString != null ? myString : null";
        String resultExpression = "!isNull(myString) ? myString : null";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "null != myString ? myString : null";
        resultExpression = "!isNull(myString) ? myString : null";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "myString != null ? myString : \"hello\"";
        resultExpression = "!isNull(myString) ? myString : \"hello\"";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "myBoolean != null ? myBoolean : null";
        resultExpression = "!isNull(myBoolean) ? (Boolean) myBoolean : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[] {"myBoolean"});

        expression = "myBoolean != null ? myBoolean : false";
        resultExpression = "!isNull(myBoolean) ? myBoolean : false";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean"});

        expression = "myBooleanObj != null ? myBooleanObj : null";
        resultExpression = "!isNull(myBooleanObj) ? myBooleanObj : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanObj"});

        expression = "myBooleanObj != null ? myBooleanObj : false";
        resultExpression = "!isNull(myBooleanObj) ? myBooleanObj : (Boolean) false";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanObj"});

        expression = "myByte != null ? myByte : null";
        resultExpression = "!isNull(myByte) ? myByte : NULL_BYTE";
        check(expression, resultExpression, byte.class, new String[] {"myByte"});

        expression = "myByte != null ? myByte : (byte) 1";
        resultExpression = "!isNull(myByte) ? myByte : byteCast(1)";
        check(expression, resultExpression, byte.class, new String[] {"myByte"});

        expression = "myShort != null ? myShort : null";
        resultExpression = "!isNull(myShort) ? myShort : NULL_SHORT";
        check(expression, resultExpression, short.class, new String[] {"myShort"});

        expression = "myShort != null ? myShort : (short) 12345";
        resultExpression = "!isNull(myShort) ? myShort : shortCast(12345)";
        check(expression, resultExpression, short.class, new String[] {"myShort"});

        expression = "myInt != null ? myInt : null";
        resultExpression = "!isNull(myInt) ? myInt : NULL_INT";
        check(expression, resultExpression, int.class, new String[] {"myInt"});

        expression = "myInt != null ? myInt : 12345";
        resultExpression = "!isNull(myInt) ? myInt : 12345";
        check(expression, resultExpression, int.class, new String[] {"myInt"});

        expression = "myFloat != null ? myFloat : null";
        resultExpression = "!isNull(myFloat) ? myFloat : NULL_FLOAT";
        check(expression, resultExpression, float.class, new String[] {"myFloat"});

        expression = "myFloat != null ? myFloat : 123.45f";
        resultExpression = "!isNull(myFloat) ? myFloat : 123.45f";
        check(expression, resultExpression, float.class, new String[] {"myFloat"});

        expression = "myLong != null ? myLong : null";
        resultExpression = "!isNull(myLong) ? myLong : NULL_LONG";
        check(expression, resultExpression, long.class, new String[] {"myLong"});

        expression = "myLong != null ? myLong : 12345L";
        resultExpression = "!isNull(myLong) ? myLong : 12345L";
        check(expression, resultExpression, long.class, new String[] {"myLong"});

        expression = "myDouble != null ? myDouble : null";
        resultExpression = "!isNull(myDouble) ? myDouble : NULL_DOUBLE";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "myDouble != null ? myDouble : 123.45d";
        resultExpression = "!isNull(myDouble) ? myDouble : 123.45d";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});
    }

    public void testOperatorOverloading() throws Exception {
        String expression = "myTestClass+1";
        String resultExpression = "plus(myTestClass, 1)";
        check(expression, resultExpression, int.class, new String[] {"myTestClass"});

        expression = "myTestClass*1.0";
        resultExpression = "multiply(myTestClass, 1.0)";
        check(expression, resultExpression, char.class, new String[] {"myTestClass"});

        expression = "myTestClass-'c'";
        resultExpression = "minus(myTestClass, 'c')";
        check(expression, resultExpression, String.class, new String[] {"myTestClass"});

        expression = "true==true";
        resultExpression = "true == true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "true!=true";
        resultExpression = "true != true";
        check(expression, resultExpression, boolean.class, new String[] {});
    }

    public void testNegation() throws Exception {
        String expression = "!myBoolean";
        String resultExpression = "!myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean"});

        expression = "!myBooleanObj";
        resultExpression = "not(myBooleanObj)";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanObj"});

        expression = "!myBoolean&&myBoolean";
        resultExpression = "!myBoolean && myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean"});

        expression = "!myBooleanObj&&myBoolean";
        resultExpression = "not(myBooleanObj) && myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBooleanObj", "myBoolean"});

        expression = "myInt!=myOtherInt";
        resultExpression = "!eq(myInt, myOtherInt)";
        check(expression, resultExpression, boolean.class, new String[] {"myInt", "myOtherInt"});

        expression = "!(myInt==myOtherInt)";
        resultExpression = "!(eq(myInt, myOtherInt))";
        check(expression, resultExpression, boolean.class, new String[] {"myInt", "myOtherInt"});

        expression = "myInt != 0 && myInt != myIntArray[myOtherInt-1]";
        resultExpression = "!eq(myInt, 0) && !eq(myInt, myIntArray[minus(myOtherInt, 1)])";
        check(expression, resultExpression, boolean.class, new String[] {"myInt", "myIntArray", "myOtherInt"});
    }

    public void testArrayOperatorOverloading() throws Exception {
        String expression = "myIntArray+myDoubleArray";
        String resultExpression = "plusArray(myIntArray, myDoubleArray)";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDoubleArray", "myIntArray"});

        expression = "myIntArray+1";
        resultExpression = "plusArray(myIntArray, 1)";
        check(expression, resultExpression, new int[0].getClass(), new String[] {"myIntArray"});

        expression = "1.0+myIntArray";
        resultExpression = "plusArray(1.0, myIntArray)";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myIntArray"});

        expression = "myIntArray==myDoubleArray";
        resultExpression = "eqArray(myIntArray, myDoubleArray)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myDoubleArray", "myIntArray"});

        expression = "myIntArray==1";
        resultExpression = "eqArray(myIntArray, 1)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myIntArray"});

        expression = "1.0==myIntArray";
        resultExpression = "eqArray(1.0, myIntArray)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myIntArray"});

        expression = "myIntArray>myDoubleArray";
        resultExpression = "greaterArray(myIntArray, myDoubleArray)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myDoubleArray", "myIntArray"});

        expression = "myIntArray>1";
        resultExpression = "greaterArray(myIntArray, 1)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myIntArray"});

        expression = "1.0>myIntArray";
        resultExpression = "greaterArray(1.0, myIntArray)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myIntArray"});

        expression = "myTestClassArray==myTestClassArray";
        resultExpression = "eqArray(myTestClassArray, myTestClassArray)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myTestClassArray"});

        expression = "myTestClassArray==myTestClass";
        resultExpression = "eqArray(myTestClassArray, myTestClass)";
        check(expression, resultExpression, new boolean[0].getClass(),
                new String[] {"myTestClass", "myTestClassArray"});

        expression = "myTestClass==myTestClassArray";
        resultExpression = "eqArray(myTestClass, myTestClassArray)";
        check(expression, resultExpression, new boolean[0].getClass(),
                new String[] {"myTestClass", "myTestClassArray"});

        expression = "myTestClassArray>myTestClassArray";
        resultExpression = "greaterArray(myTestClassArray, myTestClassArray)";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myTestClassArray"});

        expression = "myTestClassArray>myTestClass";
        resultExpression = "greaterArray(myTestClassArray, myTestClass)";
        check(expression, resultExpression, new boolean[0].getClass(),
                new String[] {"myTestClass", "myTestClassArray"});

        expression = "myTestClass>myTestClassArray";
        resultExpression = "greaterArray(myTestClass, myTestClassArray)";
        check(expression, resultExpression, new boolean[0].getClass(),
                new String[] {"myTestClass", "myTestClassArray"});
    }

    public void testArrayAccessOperatorOverloading() throws Exception {
        String expression = "myIntArray[15]";
        String resultExpression = "myIntArray[15]";
        check(expression, resultExpression, int.class, new String[] {"myIntArray"});

        expression = "myArrayList[15]";
        resultExpression = "myArrayList.get(15)";
        check(expression, resultExpression, Object.class, new String[] {"myArrayList"});

        expression = "myIntArray[myInt + 1]";
        resultExpression = "myIntArray[plus(myInt, 1)]";
        check(expression, resultExpression, int.class, new String[] {"myIntArray", "myInt"});

        expression = "myArrayList[myInt + 1]";
        resultExpression = "myArrayList.get(plus(myInt, 1))";
        check(expression, resultExpression, Object.class, new String[] {"myArrayList", "myInt"});

        expression = "myHashMap[\"test\"]";
        resultExpression = "myHashMap.get(\"test\")";
        check(expression, resultExpression, Object.class, new String[] {"myHashMap"});

        expression = "myIntVector[15]";
        resultExpression = "myIntVector.get(longCast(15))";
        check(expression, resultExpression, int.class, new String[] {"myIntVector"});

        expression = "myParameterizedArrayList[15]";
        resultExpression = "myParameterizedArrayList.get(15)";
        check(expression, resultExpression, Long.class, new String[] {"myParameterizedArrayList"});

        expression = "myParameterizedHashMap[\"test\"]";
        resultExpression = "myParameterizedHashMap.get(\"test\")";
        check(expression, resultExpression, Double.class, new String[] {"myParameterizedHashMap"});
    }

    public void testResolution() throws Exception {
        String expression = "Math.sqrt(5.0)";
        String resultExpression = "Math.sqrt(5.0)";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "Math.sqrt(5)";
        resultExpression = "Math.sqrt(doubleCast(5))";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "log(5.0)";
        resultExpression = "log(5.0)";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "java.util.Arrays.asList(5)";
        resultExpression = "java.util.Arrays.asList(5)";
        check(expression, resultExpression, List.class, new String[] {});

        expression = "io.deephaven.engine.table.ColumnDefinition.ColumnType.Normal";
        resultExpression = "io.deephaven.engine.table.ColumnDefinition.ColumnType.Normal";
        check(expression, resultExpression, ColumnType.class, new String[] {});

        expression = "ColumnDefinition.ColumnType.Normal";
        resultExpression = "ColumnDefinition.ColumnType.Normal";
        check(expression, resultExpression, ColumnType.class, new String[] {});

        expression = "Color.BLUE";
        resultExpression = "Color.BLUE";
        check(expression, resultExpression, Color.class, new String[] {});

        expression = "Color.getColor(myString)";
        resultExpression = "Color.getColor(myString)";
        check(expression, resultExpression, Color.class, new String[] {"myString"});

        expression = "NULL_INT";
        resultExpression = "NULL_INT";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "Math.sqrt(myDouble)";
        resultExpression = "Math.sqrt(myDouble)";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "Math.sqrt(myInt)";
        resultExpression = "Math.sqrt(doubleCast(myInt))";
        check(expression, resultExpression, double.class, new String[] {"myInt"});

        expression = "LanguageParserDummyClass.functionWithInterfacesAsArgTypes(`test`, 0)";
        resultExpression = "LanguageParserDummyClass.functionWithInterfacesAsArgTypes(\"test\", 0)";
        check(expression, resultExpression, int.class, new String[] {});
    }


    public void testMethodOverloading() throws Exception {
        String expression = "LanguageParserDummyClass.overloadedStaticMethod()";
        String resultExpression = "LanguageParserDummyClass.overloadedStaticMethod()";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "LanguageParserDummyClass.overloadedStaticMethod(`test`)";
        resultExpression = "LanguageParserDummyClass.overloadedStaticMethod(\"test\")";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "LanguageParserDummyClass.overloadedStaticMethod(`test1`, `test2`)";
        resultExpression = "LanguageParserDummyClass.overloadedStaticMethod(\"test1\", \"test2\")";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "myDummyClass.overloadedMethod()";
        resultExpression = "myDummyClass.overloadedMethod()";
        check(expression, resultExpression, int.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.overloadedMethod(`test`)";
        resultExpression = "myDummyClass.overloadedMethod(\"test\")";
        check(expression, resultExpression, int.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.overloadedMethod(`test1`, `test2`)";
        resultExpression = "myDummyClass.overloadedMethod(\"test1\", \"test2\")";
        check(expression, resultExpression, int.class, new String[] {"myDummyClass"});
    }

    /**
     * Test implicit argument type conversions (e.g. primitive casts and converting Vectors to Java arrays)
     */
    public void testImplicitConversion() throws Exception {
        String expression = "testVarArgs(myInt, 'a', myDouble, 1.0, 5.0, myDouble)";
        String resultExpression = "testVarArgs(myInt, 'a', new double[] { myDouble, 1.0, 5.0, myDouble })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDouble", "myInt"});

        expression = "testVarArgs(myDouble)";
        resultExpression = "testVarArgs(new double[] { myDouble })";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "testVarArgs(myDouble, myDouble)";
        resultExpression = "testVarArgs(new double[] { myDouble, myDouble })";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "testVarArgs(myInt, 'a', myDouble)";
        resultExpression = "testVarArgs(myInt, 'a', new double[] { myDouble })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDouble", "myInt"});

        expression = "testVarArgs(myInt, 'a', myDoubleArray)";
        resultExpression = "testVarArgs(myInt, 'a', myDoubleArray)";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDoubleArray", "myInt"});

        expression = "testVarArgs(myInt, 'a', myDoubleVector)";
        resultExpression = "testVarArgs(myInt, 'a', VectorConversions.nullSafeVectorToArray(myDoubleVector))";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDoubleVector", "myInt"});

        expression = "testVarArgsVector(myIntVector)";
        resultExpression = "testVarArgsVector(myIntVector)";
        check(expression, resultExpression, new IntVector[0].getClass(), new String[] {"myIntVector"});

        expression = "testVarArgsVector(myIntVector, myIntVector)";
        resultExpression = "testVarArgsVector(myIntVector, myIntVector)";
        check(expression, resultExpression, new IntVector[0].getClass(), new String[] {"myIntVector"});

        expression = "testImplicitConversion4(myInt, myVector)";
        resultExpression =
                "testImplicitConversion4(doubleCast(myInt), VectorConversions.nullSafeVectorToArray(myVector))";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector", "myInt"});

        expression = "testImplicitConversion4(myInt, myVector, myDouble)";
        resultExpression = "testImplicitConversion4(doubleCast(myInt), myVector, myDouble)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector", "myDouble", "myInt"});

        expression = "testImplicitConversion4(myInt, myDouble, myVector)";
        resultExpression = "testImplicitConversion4(doubleCast(myInt), myDouble, myVector)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector", "myDouble", "myInt"});

        // TODO: This test fails (declared arg type is "Vector...")
        // expression="testImplicitConversion5(myVector)";
        // resultExpression="testImplicitConversion5(myVector)"; // vararg of Vector -- don't convert!
        // check(expression, resultExpression, new Object[0].getClass(), new String[]{"myVector"});

        expression = "testImplicitConversion5((Vector) myVector)"; // Workaround for the above.
        resultExpression = "testImplicitConversion5((Vector) myVector)"; // vararg of Vector -- don't convert!
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector"});

        expression = "testImplicitConversion5(myVector, myVector)";
        resultExpression = "testImplicitConversion5(myVector, myVector)"; // vararg of Vector -- don't convert!
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector"});
    }

    public void testImplicitConversionByType_varargs() throws Exception {
        String expression = "testImplicitConversion_char(myChar, myChar)";
        String resultExpression = "testImplicitConversion_char(new char[] { myChar, myChar })";
        check(expression, resultExpression, new char[0].getClass(), new String[] {"myChar"});

        expression = "testImplicitConversion_byte(myByte, myByte)";
        resultExpression = "testImplicitConversion_byte(new byte[] { myByte, myByte })";
        check(expression, resultExpression, new byte[0].getClass(), new String[] {"myByte"});

        expression = "testImplicitConversion_short(myShort, myShort)";
        resultExpression = "testImplicitConversion_short(new short[] { myShort, myShort })";
        check(expression, resultExpression, new short[0].getClass(), new String[] {"myShort"});

        expression = "testImplicitConversion_int(myInt, myInt)";
        resultExpression = "testImplicitConversion_int(new int[] { myInt, myInt })";
        check(expression, resultExpression, new int[0].getClass(), new String[] {"myInt"});

        expression = "testImplicitConversion_float(myFloat, myFloat)";
        resultExpression = "testImplicitConversion_float(new float[] { myFloat, myFloat })";
        check(expression, resultExpression, new float[0].getClass(), new String[] {"myFloat"});

        expression = "testImplicitConversion_long(myLong, myLong)";
        resultExpression = "testImplicitConversion_long(new long[] { myLong, myLong })";
        check(expression, resultExpression, new long[0].getClass(), new String[] {"myLong"});

        expression = "testImplicitConversion_double(myDouble, myDouble)";
        resultExpression = "testImplicitConversion_double(new double[] { myDouble, myDouble })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDouble"});

        expression = "testImplicitConversion_boolean(true, false, myBoolean)";
        resultExpression = "testImplicitConversion_boolean(new boolean[] { true, false, myBoolean })";
        check(expression, resultExpression, new boolean[0].getClass(), new String[] {"myBoolean"});
    }

    /**
     * Ensure that vectors are converted to Java arrays when they are provided as the sole varargs argument.
     *
     * @throws Exception
     */
    public void testImplicitConversionByType_vector() throws Exception {
        String expression = "testImplicitConversion_char(myCharVector)";
        String resultExpression = "testImplicitConversion_char(VectorConversions.nullSafeVectorToArray(myCharVector))";
        check(expression, resultExpression, new char[0].getClass(), new String[] {"myCharVector"});

        expression = "testImplicitConversion_byte(myByteVector)";
        resultExpression = "testImplicitConversion_byte(VectorConversions.nullSafeVectorToArray(myByteVector))";
        check(expression, resultExpression, new byte[0].getClass(), new String[] {"myByteVector"});

        expression = "testImplicitConversion_short(myShortVector)";
        resultExpression = "testImplicitConversion_short(VectorConversions.nullSafeVectorToArray(myShortVector))";
        check(expression, resultExpression, new short[0].getClass(), new String[] {"myShortVector"});

        expression = "testImplicitConversion_int(myIntVector)";
        resultExpression = "testImplicitConversion_int(VectorConversions.nullSafeVectorToArray(myIntVector))";
        check(expression, resultExpression, new int[0].getClass(), new String[] {"myIntVector"});

        expression = "testImplicitConversion_float(myFloatVector)";
        resultExpression = "testImplicitConversion_float(VectorConversions.nullSafeVectorToArray(myFloatVector))";
        check(expression, resultExpression, new float[0].getClass(), new String[] {"myFloatVector"});

        expression = "testImplicitConversion_long(myLongVector)";
        resultExpression = "testImplicitConversion_long(VectorConversions.nullSafeVectorToArray(myLongVector))";
        check(expression, resultExpression, new long[0].getClass(), new String[] {"myLongVector"});

        expression = "testImplicitConversion_double(myDoubleVector)";
        resultExpression = "testImplicitConversion_double(VectorConversions.nullSafeVectorToArray(myDoubleVector))";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDoubleVector"});

        expression = "testImplicitConversion_Object(myVector)";
        resultExpression = "testImplicitConversion_Object(VectorConversions.nullSafeVectorToArray(myVector))";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector"});

        expression = "testImplicitConversion_Object(myVector)";
        resultExpression = "testImplicitConversion_Object(VectorConversions.nullSafeVectorToArray(myVector))";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector"});
    }

    public void testImplicitConversion_Object() throws Exception {
        String expression = "testImplicitConversion_Object(myVector, myVector)";
        String resultExpression = "testImplicitConversion_Object(myVector, myVector)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myVector"});

        expression = "testImplicitConversion_Object(myLongArray)";
        resultExpression = "testImplicitConversion_Object(myLongArray)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myLongArray"});

        expression = "testImplicitConversion_Object(myDoubleArray)";
        resultExpression = "testImplicitConversion_Object(myDoubleArray)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleArray"});

        // TODO: #3266 vector arguments to Object varargs methods are handled inconsistently
        expression = "testImplicitConversion_Object(myDoubleVector)";
        resultExpression = "testImplicitConversion_Object(VectorConversions.nullSafeVectorToArray(myDoubleVector))";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleVector"});

        // expression = "testImplicitConversion_Object(myDoubleVector, myInt)";
        // resultExpression =
        // "testImplicitConversion_Object(VectorConversions.nullSafeVectorToArray(myDoubleVector), myInt)";
        // check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleVector", "myInt"});
        //
        // expression = "testImplicitConversion_Object(myInt, myDoubleVector)";
        // resultExpression =
        // "testImplicitConversion_Object(myInt, VectorConversions.nullSafeVectorToArray(myDoubleVector))";
        // check(expression, resultExpression, new Object[0].getClass(), new String[] {"myInt", "myDoubleVector"});
        //
        // expression = "testImplicitConversion_Object(myDoubleVector, myIntVector)";
        // resultExpression =
        // "testImplicitConversion_Object(VectorConversions.nullSafeVectorToArray(myDoubleVector),
        // VectorConversions.nullSafeVectorToArray(myIntVector))";
        // check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleVector",
        // "myIntVector"});

        expression = "testImplicitConversion_Object(myDoubleArray, myInt)";
        resultExpression = "testImplicitConversion_Object(myDoubleArray, myInt)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleArray", "myInt"});
    }

    public void testExplicitClosureCall() throws Exception {
        String expression = "myClosure.call()";
        String resultExpression = "myClosure.call()";
        check(expression, resultExpression, Object.class, new String[] {"myClosure"});

        expression = "myClosure.call(1)";
        resultExpression = "myClosure.call(1)";
        check(expression, resultExpression, Object.class, new String[] {"myClosure"});

        expression = "myClosure.call(1, 2, 3)";
        resultExpression = "myClosure.call(1, 2, 3)";
        check(expression, resultExpression, Object.class, new String[] {"myClosure"});
    }

    public void testImplicitClosureCall() throws Exception {
        String expression = "myClosure()";
        String resultExpression = "myClosure.call()";
        check(expression, resultExpression, Object.class, new String[] {"myClosure"});

        expression = "myClosure(1)";
        resultExpression = "myClosure.call(1)";
        check(expression, resultExpression, Object.class, new String[] {"myClosure"});

        expression = "myClosure(1, 2, 3)";
        resultExpression = "myClosure.call(1, 2, 3)";
        check(expression, resultExpression, Object.class, new String[] {"myClosure"});
    }

    public void testPyObject() throws Exception {
        String expression = "myPyObject";
        String resultExpression = "myPyObject";
        check(expression, resultExpression, PyObject.class, new String[] {"myPyObject"});

        expression = "myPyObject.myField";
        resultExpression = "myPyObject.getAttribute(\"myField\")";
        check(expression, resultExpression, PyObject.class, new String[] {"myPyObject"});

        expression = "myPyObject.getAttribute(`myField`)";
        resultExpression = "myPyObject.getAttribute(\"myField\")";
        check(expression, resultExpression, PyObject.class, new String[] {"myPyObject"});

        expression = "myPyObject.getIntValue()";
        resultExpression = "myPyObject.getIntValue()";
        check(expression, resultExpression, int.class, new String[] {"myPyObject"});

        expression = "myPyObject.getLongValue()";
        resultExpression = "myPyObject.getLongValue()";
        check(expression, resultExpression, long.class, new String[] {"myPyObject"});

        expression = "myPyObject.getBooleanValue()";
        resultExpression = "myPyObject.getBooleanValue()";
        check(expression, resultExpression, boolean.class, new String[] {"myPyObject"});

        expression = "myPyObject.getDoubleValue()";
        resultExpression = "myPyObject.getDoubleValue()";
        check(expression, resultExpression, double.class, new String[] {"myPyObject"});

        expression = "myPyObject.getStringValue()";
        resultExpression = "myPyObject.getStringValue()";
        check(expression, resultExpression, String.class, new String[] {"myPyObject"});

        expression = "myPyObject.getObjectValue()";
        resultExpression = "myPyObject.getObjectValue()";
        check(expression, resultExpression, Object.class, new String[] {"myPyObject"});

        expression = "myPyObject.getType()";
        resultExpression = "myPyObject.getType()";
        check(expression, resultExpression, PyObject.class, new String[] {"myPyObject"});

        expression = "myPyObject.isDict()";
        resultExpression = "myPyObject.isDict()";
        check(expression, resultExpression, boolean.class, new String[] {"myPyObject"});

        expression = "myPyObject.isList()";
        resultExpression = "myPyObject.isList()";
        check(expression, resultExpression, boolean.class, new String[] {"myPyObject"});
    }

    public void testPyCallable() throws Exception {
        String expression = "myPyCallable.FIELD";
        String resultExpression = "myPyCallable.getAttribute(\"FIELD\")";
        check(expression, resultExpression, PyObject.class, new String[] {"myPyCallable"});

        //@formatter:off
        /*
         * This is based on a test case test_callable_attrs_in_query():
         *
         * foo = Foo() # This 'foo' instance is a PyCallableWrapper, not a PyObject
         * rt = empty_table(1).update("Col = (int)foo.do_something_instance()")
         * self.assertTrue(rt.columns[0].data_type == dtypes.int32)
         */
        //@formatter:on
        expression = "myPyCallable.pyMethod()";
        resultExpression =
                "(new io.deephaven.engine.table.impl.lang.PyCallableWrapperDummyImpl(myPyCallable.getAttribute(\"pyMethod\"))).call()";
        // the result is an Object, not a PyObject -- Object is the return type of PyCallableWrapper.getAttribute().
        check(expression, resultExpression, Object.class, new String[] {"myPyCallable"});

        expression = "(int)myPyCallable.FIELD";
        resultExpression = "intPyCast(myPyCallable.getAttribute(\"FIELD\"))";
        check(expression, resultExpression, int.class, new String[] {"myPyCallable"});

        expression = "myPyCallable.getAttribute(\"FIELD\")";
        resultExpression = "myPyCallable.getAttribute(\"FIELD\")";
        check(expression, resultExpression, PyObject.class, new String[] {"myPyCallable"});
    }

    /**
     * Test converting implicit python calls into explicit ones.
     */
    public void testImplicitPythonCallNoScope() throws Exception {
        final PyCallableWrapper mockPyCallable0 = getMockPyCallable();
        final PyCallableWrapper mockPyCallable1 = getMockPyCallable(int.class);
        final PyCallableWrapper mockPyCallable3 = getMockPyCallable(int.class, int.class, int.class);

        variables.put("myIntConstant", int.class);
        variables.put("myIntCol", int.class);
        variables.put("myPyCallable0", PyCallableWrapper.class);
        variables.put("myPyCallable1", PyCallableWrapper.class);
        variables.put("myPyCallable3", PyCallableWrapper.class);

        try (SafeCloseable ignored = TestExecutionContext.createForUnitTests().open()) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
            ExecutionContext.getContext().getQueryScope().putParam("myPyCallable0", mockPyCallable0);
            ExecutionContext.getContext().getQueryScope().putParam("myPyCallable1", mockPyCallable1);
            ExecutionContext.getContext().getQueryScope().putParam("myPyCallable3", mockPyCallable3);

            ExecutionContext.getContext().getQueryScope().putParam("myInt", 42);

            String expression = "myPyCallable0()";
            String resultExpression = "myPyCallable0.call()";
            check(expression, resultExpression, Object.class, new String[] {"myPyCallable0"});

            expression = "myPyCallable1(1)";
            resultExpression = "myPyCallable1.call(1)";
            check(expression, resultExpression, Object.class, new String[] {"myPyCallable1"});

            expression = "myPyCallable3(1, 2, 3)";
            resultExpression = "myPyCallable3.call(1, 2, 3)";
            check(expression, resultExpression, Object.class, new String[] {"myPyCallable3"});

            expression = "myPyCallable3(myIntConstant, 2, 3)";
            resultExpression = "myPyCallable3.call(myIntConstant, 2, 3)";
            check(expression, resultExpression, Object.class, new String[] {"myPyCallable3", "myIntConstant"});

            // prepareVectorizationArgs
            expression = "myPyCallable3(myIntCol, 2, 3)";
            resultExpression = "myPyCallable3.call(myIntCol, 2, 3)";
            check(expression, resultExpression, Object.class, new String[] {"myPyCallable3", "myIntCol"});
        }
    }

    private PyCallableWrapper getMockPyCallable(Class<?>... parameterTypes) {
        return new PyCallableWrapperDummyImpl(List.of(parameterTypes));
    }

    /**
     * Test converting implicit python calls into explicit ones.
     */
    public void testImplicitPythonCallWithScope() throws Exception {
        try (SafeCloseable ignored = TestExecutionContext.createForUnitTests().open()) {
            String expression = "myPyObject.myPyMethod()";
            String resultExpression =
                    "(new io.deephaven.engine.table.impl.lang.PyCallableWrapperDummyImpl(myPyObject.getAttribute(\"myPyMethod\"))).call()";
            check(expression, resultExpression, Object.class, new String[] {"myPyObject"});

            expression = "myPyObject.myPyMethod(1)";
            resultExpression =
                    "(new io.deephaven.engine.table.impl.lang.PyCallableWrapperDummyImpl(myPyObject.getAttribute(\"myPyMethod\"))).call(1)";
            check(expression, resultExpression, Object.class, new String[] {"myPyObject"});

            expression = "myPyObject.myPyMethod(1, 2, 3)";
            resultExpression =
                    "(new io.deephaven.engine.table.impl.lang.PyCallableWrapperDummyImpl(myPyObject.getAttribute(\"myPyMethod\"))).call(1, 2, 3)";
            check(expression, resultExpression, Object.class, new String[] {"myPyObject"});
        }
    }

    /**
     * Test calling the default methods from {@link Object}. (In the past, these were not recognized on interfaces.)
     */
    public void testObjectMethods() throws Exception {
        // Call hashCode() on an Object
        String expression = "myObject.hashCode()";
        String resultExpression = "myObject.hashCode()";
        check(expression, resultExpression, int.class, new String[] {"myObject"});

        // Call hashCode() on a subclass of Object
        expression = "myString.hashCode()";
        resultExpression = "myString.hashCode()";
        check(expression, resultExpression, int.class, new String[] {"myString"});

        // Call hashCode() on an interface
        expression = "myCharSequence.hashCode()";
        resultExpression = "myCharSequence.hashCode()";
        check(expression, resultExpression, int.class, new String[] {"myCharSequence"});

        // Call hashCode() on a String literal
        expression = "`str`.hashCode()";
        resultExpression = "\"str\".hashCode()";
        check(expression, resultExpression, int.class, new String[] {});

        // Make sure calling hashCode() does not work on a primitive
        try {
            expression = "(42).hashCode()";
            resultExpression = "(42).hashCode()";
            check(expression, resultExpression, int.class, new String[] {});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }
    }

    public void testFieldAccess() throws Exception {

        String expression = "`a_b_c_d_e`.split(`_`).length";
        String resultExpression = "\"a_b_c_d_e\".split(\"_\").length";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "new LanguageParserDummyClass().value";
        resultExpression = "new LanguageParserDummyClass().value";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "LanguageParserDummyClass.StaticNestedClass.staticVar";
        resultExpression = "LanguageParserDummyClass.StaticNestedClass.staticVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "LanguageParserDummyClass.StaticNestedClass.staticInstanceOfStaticClass.instanceVar";
        resultExpression = "LanguageParserDummyClass.StaticNestedClass.staticInstanceOfStaticClass.instanceVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "new LanguageParserDummyClass.StaticNestedClass().instanceVar";
        resultExpression = "new LanguageParserDummyClass.StaticNestedClass().instanceVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "myDummyClass.InnerClass";
        resultExpression = "myDummyClass.InnerClass";
        check(expression, resultExpression, LanguageParserDummyClass.InnerClass.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClassInstance.staticVar";
        resultExpression = "myDummyClass.innerClassInstance.staticVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyClass"});

        expression = "myDummyInnerClass.staticVar";
        resultExpression = "myDummyInnerClass.staticVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});

        expression = "myDummyInnerClass.instanceVar";
        resultExpression = "myDummyInnerClass.instanceVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});

        expression = "myDummyClass.innerClassInstance.instanceVar";
        resultExpression = "myDummyClass.innerClassInstance.instanceVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClassInstance.innerInnerClassInstance";
        resultExpression = "myDummyClass.innerClassInstance.innerInnerClassInstance";
        check(expression, resultExpression, LanguageParserDummyClass.InnerClass.InnerInnerClass.class,
                new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClassInstance.innerInnerClassInstance.innerInnerInstanceVar";
        resultExpression = "myDummyClass.innerClassInstance.innerInnerClassInstance.innerInnerInstanceVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClass2Instance.innerClassAsInstanceOfAnotherInnerClass";
        resultExpression = "myDummyClass.innerClass2Instance.innerClassAsInstanceOfAnotherInnerClass";
        check(expression, resultExpression, LanguageParserDummyClass.InnerClass.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClass2Instance.innerClassAsInstanceOfAnotherInnerClass.instanceVar";
        resultExpression = "myDummyClass.innerClass2Instance.innerClassAsInstanceOfAnotherInnerClass.instanceVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyClass"});

        expression = "myDoubleArray.length";
        resultExpression = "myDoubleArray.length";
        check(expression, resultExpression, int.class, new String[] {"myDoubleArray"});

        expression = "myDoubleArray[myDoubleArray.length]";
        resultExpression = "myDoubleArray[myDoubleArray.length]";
        check(expression, resultExpression, double.class, new String[] {"myDoubleArray"});

        expression = "myDoubleArray[myDoubleArray.length-1]";
        resultExpression = "myDoubleArray[minus(myDoubleArray.length, 1)]";
        check(expression, resultExpression, double.class, new String[] {"myDoubleArray"});

        expression = "myTestClassArray[0].var";
        resultExpression = "myTestClassArray[0].var";
        check(expression, resultExpression, int.class, new String[] {"myTestClassArray"});
    }

    /**
     * Test bad field access expressions
     */
    public void testBadFieldAccess() throws Exception {
        String expression, resultExpression;

        PropertySaver p = new PropertySaver();
        p.setProperty("QueryLanguageParser.verboseExceptionMessages", "false"); // Better to test with non-verbose
                                                                                // messages
        try {
            // First, test just bad field name
            try {
                expression = "myDummyInnerClass.staticVarThatDoesNotExist";
                resultExpression = "myDummyInnerClass.staticVarThatDoesNotExist";
                check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});
                fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
            } catch (QueryLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : myDummyInnerClass") ||
                        !ex.getMessage().contains("Field Name : staticVarThatDoesNotExist")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }

            // Then do the same thing on a class name (not a variable)
            try {
                expression = "LanguageParserDummyClass.StaticNestedClass.staticVarThatDoesNotExist";
                resultExpression = "LanguageParserDummyClass.StaticNestedClass.staticVarThatDoesNotExist";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
            } catch (QueryLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : LanguageParserDummyClass.StaticNestedClass") ||
                        !ex.getMessage().contains("Field Name : staticVarThatDoesNotExist")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }

            // Next, test bad scope
            try {
                expression = "myDummyNonExistentInnerClass.staticVar";
                resultExpression = "myDummyNonExistentInnerClass.staticVar";
                check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});
                fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
            } catch (QueryLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : myDummyNonExistentInnerClass") ||
                        !ex.getMessage().contains("Field Name : staticVar")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }


            try {
                expression = "LanguageParserNonExistentDummyClass.StaticNestedClass.staticVar";
                resultExpression = "LanguageParserNonExistentDummyClass.StaticNestedClass.staticVar";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
            } catch (QueryLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : LanguageParserNonExistentDummyClass.StaticNestedClass") ||
                        !ex.getMessage().contains("Field Name : staticVar")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }



            /*
             * Also test within a method call. This is essentially the case that prompted the fix. The actual issue with
             * the expression is that the enclosing class name is omitted when trying to access the nested enum
             * (NestedEnum.ONE), but the user experience was poor because the exception was very unclear.
             *
             * There is a test of the proper expression in testComplexExpressions().
             */
            try {
                expression =
                        "io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.interpolate(myDoubleVector.toArray(),myDoubleVector.toArray(),new double[]{myDouble},io.deephaven.engine.table.impl.lang.NestedEnum.ONE,false)[0]";
                resultExpression =
                        "io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.interpolate(myDoubleVector.toArray(),myDoubleVector.toArray(),new double[]{myDouble},io.deephaven.engine.table.impl.lang.NestedEnum.ONE,false)[0]";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
            } catch (QueryLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : io.deephaven.engine.table.impl.lang.NestedEnum") ||
                        !ex.getMessage().contains("Field Name : ONE")) {
                    fail("Useless exception message!\n\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }

            // Ensure that when we can't resolve a field, we explicitly state the scope type. (We've struggled
            // supporting customers in the past when the scope type was unclear.)
            try {
                expression = "myTable[myTable.length-1]";
                resultExpression = "myTable[minus(myTable.length, 1)]";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
            } catch (QueryLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : myTable") ||
                        !ex.getMessage().contains("Scope Type : " + Table.class.getCanonicalName()) ||
                        !ex.getMessage().contains("Field Name : length")) {
                    fail("Useless exception message!\n\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }


        } finally {
            p.restore();
        }
    }

    public void testEnums() throws Exception {
        String expression = "myEnumValue";
        String resultExpression = "myEnumValue";
        check(expression, resultExpression, LanguageParserDummyEnum.class, new String[] {"myEnumValue"});

        expression = "myEnumValue.getAttribute()";
        resultExpression = "myEnumValue.getAttribute()";
        check(expression, resultExpression, String.class, new String[] {"myEnumValue"});

        expression = "LanguageParserDummyEnum.ONE";
        resultExpression = "LanguageParserDummyEnum.ONE";
        check(expression, resultExpression, LanguageParserDummyEnum.class, new String[] {});

        expression = "LanguageParserDummyEnum.ONE.getAttribute()";
        resultExpression = "LanguageParserDummyEnum.ONE.getAttribute()";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "LanguageParserDummyInterface.AnEnum.THING_ONE";
        resultExpression = "LanguageParserDummyInterface.AnEnum.THING_ONE";
        check(expression, resultExpression, LanguageParserDummyInterface.AnEnum.class, new String[] {});

        expression = "io.deephaven.engine.table.impl.lang.LanguageParserDummyInterface.AnEnum.THING_ONE";
        resultExpression = "io.deephaven.engine.table.impl.lang.LanguageParserDummyInterface.AnEnum.THING_ONE";
        check(expression, resultExpression, LanguageParserDummyInterface.AnEnum.class, new String[] {});

        expression = "LanguageParserDummyClass.SubclassOfLanguageParserDummyClass.EnumInInterface.THING_ONE";
        resultExpression = "LanguageParserDummyClass.SubclassOfLanguageParserDummyClass.EnumInInterface.THING_ONE";
        check(expression, resultExpression,
                LanguageParserDummyClass.SubclassOfLanguageParserDummyClass.EnumInInterface.class, new String[] {});

        expression = "LanguageParserDummyClass.functionWithEnumAsArgs(LanguageParserDummyEnum.ONE)";
        resultExpression = "LanguageParserDummyClass.functionWithEnumAsArgs(LanguageParserDummyEnum.ONE)";
        check(expression, resultExpression, int.class, new String[] {});

        expression =
                "LanguageParserDummyClass.functionWithEnumAsArgs(LanguageParserDummyInterface.AnEnum.THING_ONE)";
        resultExpression =
                "LanguageParserDummyClass.functionWithEnumAsArgs(LanguageParserDummyInterface.AnEnum.THING_ONE)";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "LanguageParserDummyClass.functionWithEnumVarArgs(myEnumValue, LanguageParserDummyEnum.ONE)";
        resultExpression =
                "LanguageParserDummyClass.functionWithEnumVarArgs(myEnumValue, LanguageParserDummyEnum.ONE)";
        check(expression, resultExpression, int.class, new String[] {"myEnumValue"});
    }

    public void testBoxing() throws Exception {
        String expression = "myIntObj";
        String resultExpression = "myIntObj";
        check(expression, resultExpression, Integer.class, new String[] {"myIntObj"});

        expression = "LanguageParserDummyClass.boxedIntResult()";
        resultExpression = "LanguageParserDummyClass.boxedIntResult()";
        check(expression, resultExpression, Integer.class, new String[] {});

        expression = "1+myIntObj";
        resultExpression = "plus(1, myIntObj.intValue())";
        check(expression, resultExpression, int.class, new String[] {"myIntObj"});

        expression = "1*myDoubleObj";
        resultExpression = "multiply(1, myDoubleObj.doubleValue())";
        check(expression, resultExpression, double.class, new String[] {"myDoubleObj"});

        expression = "myInt/myLongObj";
        resultExpression = "divide(myInt, myLongObj.longValue())";
        check(expression, resultExpression, double.class, new String[] {"myInt", "myLongObj"});

        expression = "myIntObj/myLong";
        resultExpression = "divide(myIntObj.intValue(), myLong)";
        check(expression, resultExpression, double.class, new String[] {"myIntObj", "myLong"});

        expression = "myDoubleObj+myLongObj+3*4";
        resultExpression = "plus(plus(myDoubleObj.doubleValue(), myLongObj.longValue()), multiply(3, 4))";
        check(expression, resultExpression, double.class, new String[] {"myDoubleObj", "myLongObj"});

        expression = "1==myIntObj";
        resultExpression = "eq(1, myIntObj.intValue())";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj"});

        expression = "myInt>1+2*myIntObj/4 || myBooleanObj";
        resultExpression = "greater(myInt, plus(1, divide(multiply(2, myIntObj.intValue()), 4))) || myBooleanObj";
        check(expression, resultExpression, boolean.class, new String[] {"myBooleanObj", "myInt", "myIntObj"});

        expression = "myIntObj+myString";
        resultExpression = "myIntObj + myString";
        check(expression, resultExpression, String.class, new String[] {"myIntObj", "myString"});

        expression = "myIntObj>2";
        resultExpression = "greater(myIntObj.intValue(), 2)";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj"});
    }

    public void testUnboxAndWiden() throws Exception {
        // ensure we can find the original method
        String expression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myLong)";
        String resultExpression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myLong)";
        check(expression, resultExpression, Instant.class, new String[] {"myInstant", "myLong"});

        // check long unbox
        expression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myLongObj)";
        resultExpression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myLongObj.longValue())";
        check(expression, resultExpression, Instant.class, new String[] {"myInstant", "myLongObj"});

        // check int widen
        expression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myInt)";
        resultExpression = "io.deephaven.time.DateTimeUtils.plus(myInstant, longCast(myInt))";
        check(expression, resultExpression, Instant.class, new String[] {"myInstant", "myInt"});

        // check int unbox and widen
        expression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myIntObj)";
        resultExpression = "io.deephaven.time.DateTimeUtils.plus(myInstant, myIntObj.longValue())";
        check(expression, resultExpression, Instant.class, new String[] {"myInstant", "myIntObj"});

        // check vararg widen
        expression = "testImplicitConversion_double(myFloat, myFloat)";
        resultExpression = "testImplicitConversion_double(new double[] { doubleCast(myFloat), doubleCast(myFloat) })";
        check(expression, resultExpression, double[].class, new String[] {"myFloat"});

        // check vararg unbox and widen
        expression = "testImplicitConversion_double(myFloatObj, myFloatObj)";
        resultExpression =
                "testImplicitConversion_double(new double[] { myFloatObj.doubleValue(), myFloatObj.doubleValue() })";
        check(expression, resultExpression, double[].class, new String[] {"myFloatObj"});

        // check object array super casting
        expression = resultExpression = "testImplicitConversionArraySuper(new Integer[] { 1, 2, 3, 4 })";
        check(expression, resultExpression, Number[].class, new String[] {});

        // check object array super casting
        expression =
                resultExpression = "testImplicitConversionNestedArraySuper(new Integer[][] { new Integer[] { 1 } })";
        check(expression, resultExpression, Number[][].class, new String[] {});

        // See (deephaven-core#1201): do not widen primitive array elements
        try {
            expression = resultExpression = "testImplicitConversion_double(new float[]{ myFloat })";
            check(expression, resultExpression, double[].class, new String[] {"myFloat"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException err) {
            Assert.eqTrue(err.getMessage().contains("Cannot find method"),
                    "err.getMessage().contains(\"Cannot find method\")");
        }

        // See (deephaven-core#1201): do not unbox array elements
        try {
            expression = resultExpression = "testImplicitConversion_double(new Double[]{ myDoubleObj })";
            check(expression, resultExpression, double[].class, new String[] {"myDoubleObj"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException err) {
            Assert.eqTrue(err.getMessage().contains("Cannot find method"),
                    "err.getMessage().contains(\"Cannot find method\")");
        }

        // See (deephaven-core#1201): do not unbox and widen array elements
        try {
            expression = resultExpression = "testImplicitConversion_double(new Float[]{ myFloatObj })";
            check(expression, resultExpression, double[].class, new String[] {"myFloatObj"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException err) {
            Assert.eqTrue(err.getMessage().contains("Cannot find method"),
                    "err.getMessage().contains(\"Cannot find method\")");
        }
    }

    public void testEqualsConversion() throws Exception {
        QueryLanguageParser.Result result =
                new QueryLanguageParser("1==1", null, null, staticImports, null, null).getResult();
        assertEquals("eq(1, 1)", result.getConvertedExpression());

        result = new QueryLanguageParser("1=1", null, null, staticImports, null, null).getResult();
        assertEquals("eq(1, 1)", result.getConvertedExpression());

        result = new QueryLanguageParser("`me`=`you`", null, null, staticImports, null, null).getResult();
        assertEquals("eq(\"me\", \"you\")", result.getConvertedExpression());

        result = new QueryLanguageParser("1=1 || 2=2 && (3=3 && 4==4)", null, null, staticImports, null, null)
                .getResult();
        assertEquals("eq(1, 1) || eq(2, 2) && (eq(3, 3) && eq(4, 4))", result.getConvertedExpression());

        result = new QueryLanguageParser("1<=1", null, null, staticImports, null, null).getResult();
        assertEquals("lessEquals(1, 1)", result.getConvertedExpression());

        result = new QueryLanguageParser("1>=1", null, null, staticImports, null, null).getResult();
        assertEquals("greaterEquals(1, 1)", result.getConvertedExpression());

        result = new QueryLanguageParser("1!=1", null, null, staticImports, null, null).getResult();
        assertEquals("!eq(1, 1)", result.getConvertedExpression());
    }

    /**
     * In order to support the null values defined in {@link QueryConstants}, language parser converts the equality and
     * relational operators into method calls.
     */
    public void testComparisonConversion() throws Exception {
        String expression = "myTestClass>myIntObj";
        String resultExpression = "greater(myTestClass, myIntObj.intValue())";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj", "myTestClass"});

        expression = "myTestClass>=myIntObj";
        resultExpression = "greaterEquals(myTestClass, myIntObj.intValue())";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj", "myTestClass"});

        expression = "myTestClass<myIntObj";
        resultExpression = "less(myTestClass, myIntObj.intValue())";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj", "myTestClass"});

        expression = "myTestClass<=myIntObj";
        resultExpression = "lessEquals(myTestClass, myIntObj.intValue())";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj", "myTestClass"});

        expression = "myTestClass>myTestClass";
        resultExpression = "greater(myTestClass, myTestClass)";
        check(expression, resultExpression, boolean.class, new String[] {"myTestClass"});

        expression = "myTestClass>=myTestClass";
        resultExpression = "greaterEquals(myTestClass, myTestClass)";
        check(expression, resultExpression, boolean.class, new String[] {"myTestClass"});

        expression = "myTestClass<myTestClass";
        resultExpression = "less(myTestClass, myTestClass)";
        check(expression, resultExpression, boolean.class, new String[] {"myTestClass"});

        expression = "myTestClass<=myTestClass";
        resultExpression = "lessEquals(myTestClass, myTestClass)";
        check(expression, resultExpression, boolean.class, new String[] {"myTestClass"});

        expression = "myTestClass==myTestClass";
        resultExpression = "eq(myTestClass, myTestClass)";
        check(expression, resultExpression, boolean.class, new String[] {"myTestClass"});

        expression = "myTestClass!=myTestClass";
        resultExpression = "!eq(myTestClass, myTestClass)";
        check(expression, resultExpression, boolean.class, new String[] {"myTestClass"});
    }

    public void testArrayAllocation() throws Exception {
        String expression = "new Integer[5]";
        String resultExpression = "new Integer[5]";
        check(expression, resultExpression, new Integer[0].getClass(), new String[] {});

        expression = "new int[5]";
        resultExpression = "new int[5]";
        check(expression, resultExpression, new int[0].getClass(), new String[] {});

        expression = "new int[4][2]";
        resultExpression = "new int[4][2]";
        check(expression, resultExpression, new int[0][0].getClass(), new String[] {});

        expression = "new int[]{ 1, 2, 3, 4 }";
        resultExpression = "new int[] { 1, 2, 3, 4 }";
        check(expression, resultExpression, new int[0].getClass(), new String[] {});

        expression = "new int[] {1,2,3,4}";
        resultExpression = "new int[] { 1, 2, 3, 4 }"; // note that the parser alters spacing
        check(expression, resultExpression, new int[0].getClass(), new String[] {});

        expression = "new String[]{ `This`, `is`, `a`, `test` }";
        resultExpression = "new String[] { \"This\", \"is\", \"a\", \"test\" }";
        check(expression, resultExpression, new String[0].getClass(), new String[] {});

        expression = "new SubclassOfLanguageParserDummyClass[] { " +
                "LanguageParserDummyClass.innerClassInstance, " +
                "LanguageParserDummyClass.innerClass2Instance, " +
                "new LanguageParserDummyClass.StaticNestedClass(), " +
                "myDummyInnerClass }";
        resultExpression = "new SubclassOfLanguageParserDummyClass[] { " +
                "LanguageParserDummyClass.innerClassInstance, " +
                "LanguageParserDummyClass.innerClass2Instance, " +
                "new LanguageParserDummyClass.StaticNestedClass(), " +
                "myDummyInnerClass }";
        check(expression, resultExpression,
                new LanguageParserDummyClass.SubclassOfLanguageParserDummyClass[0].getClass(),
                new String[] {"myDummyInnerClass"});
    }

    public void testArraysAsArguments() throws Exception {
        String expression = "LanguageParserDummyClass.arrayAndVectorFunction(myIntVector)";
        String resultExpression = "LanguageParserDummyClass.arrayAndVectorFunction(myIntVector)";
        check(expression, resultExpression, long.class, new String[] {"myIntVector"});

        expression = "LanguageParserDummyClass.arrayAndVectorFunction(myIntArray)";
        resultExpression = "LanguageParserDummyClass.arrayAndVectorFunction(myIntArray)";
        check(expression, resultExpression, long.class, new String[] {"myIntArray"});

        expression = "LanguageParserDummyClass.arrayOnlyFunction(myIntVector)";
        resultExpression =
                "LanguageParserDummyClass.arrayOnlyFunction(VectorConversions.nullSafeVectorToArray(myIntVector))";
        check(expression, resultExpression, long.class, new String[] {"myIntVector"});

        expression = "LanguageParserDummyClass.vectorOnlyFunction(myIntArray)";
        resultExpression = "LanguageParserDummyClass.vectorOnlyFunction(myIntArray)";
        try {
            // We don't (currently?) support converting primitive arrays to Vectors.
            check(expression, resultExpression, int.class, new String[] {"myIntArray"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ex) {
            // exception expected
        }
    }

    public void testObjectConstruction() throws Exception {
        String expression = "new Integer(myInt)";
        String resultExpression = "new Integer(myInt)";
        check(expression, resultExpression, Integer.class, new String[] {"myInt"});

        expression = "new String(`Hello, world!`)";
        resultExpression = "new String(\"Hello, world!\")";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "new String(new char[]{ 'a', 'b', 'c', 'd', 'e' }, 1, 4)";
        resultExpression = "new String(new char[] { 'a', 'b', 'c', 'd', 'e' }, 1, 4)";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "new HashSet()";
        resultExpression = "new HashSet()";
        check(expression, resultExpression, HashSet.class, new String[] {});

        expression = "new HashSet<String>()";
        resultExpression = "new HashSet<String>()";
        check(expression, resultExpression, HashSet.class, new String[] {});

        expression = "new io.deephaven.engine.table.impl.lang.LanguageParserDummyClass()";
        resultExpression = "new io.deephaven.engine.table.impl.lang.LanguageParserDummyClass()";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {});

        expression = "new LanguageParserDummyClass()";
        resultExpression = "new LanguageParserDummyClass()";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {});

        expression = "new LanguageParserDummyClass(myInt)";
        resultExpression = "new LanguageParserDummyClass(myInt)";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {"myInt"});

        expression = "new LanguageParserDummyClass(myLong)";
        resultExpression = "new LanguageParserDummyClass(myLong)";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {"myLong"});

        expression = "new LanguageParserDummyClass(myLongObj)";
        resultExpression = "new LanguageParserDummyClass(myLongObj)";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {"myLongObj"});

        expression = "new LanguageParserDummyClass.StaticNestedClass()";
        resultExpression = "new LanguageParserDummyClass.StaticNestedClass()";
        check(expression, resultExpression, LanguageParserDummyClass.StaticNestedClass.class, new String[] {});

        expression = "io.deephaven.time.DateTimeUtils.epochNanosToInstant(123L)";
        resultExpression = "io.deephaven.time.DateTimeUtils.epochNanosToInstant(123L)";
        check(expression, resultExpression, Instant.class, new String[] {});
    }

    public void testIntToLongConversion() throws Exception {
        String expression = "1+1283209200466";
        String resultExpression = "plus(1, 1283209200466L)";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "1 + -1283209200466";
        resultExpression = "plus(1, negate(1283209200466L))";
        check(expression, resultExpression, long.class, new String[] {});
    }

    public void testGenericMethods() throws Exception {
        String expression = "genericSingleToSingle(myDoubleObj)";
        String resultExpression = "genericSingleToSingle(myDoubleObj)";
        check(expression, resultExpression, Double.class, new String[] {"myDoubleObj"});

        expression = "genericArrayToArray(myDoubleObjArray)";
        resultExpression = "genericArrayToArray(myDoubleObjArray)";
        check(expression, resultExpression, new Double[0].getClass(), new String[] {"myDoubleObjArray"});

        expression = "genericArrayToSingle(myDoubleObjArray)";
        resultExpression = "genericArrayToSingle(myDoubleObjArray)";
        check(expression, resultExpression, Double.class, new String[] {"myDoubleObjArray"});

        expression = "genericSingleToArray(myDoubleObj)";
        resultExpression = "genericSingleToArray(myDoubleObj)";
        check(expression, resultExpression, new Double[0].getClass(), new String[] {"myDoubleObj"});

        expression = "genericSingleToDoubleArray(myDoubleObj)";
        resultExpression = "genericSingleToDoubleArray(myDoubleObj)";
        check(expression, resultExpression, new Double[0][0].getClass(), new String[] {"myDoubleObj"});

        expression = "genericDoubleArrayToSingle(new Double[0][0])";
        resultExpression = "genericDoubleArrayToSingle(new Double[0][0])";
        check(expression, resultExpression, Double.class, new String[] {});

        expression = "genericGetKey(myParameterizedHashMap)";
        resultExpression = "genericGetKey(myParameterizedHashMap)";
        check(expression, resultExpression, Integer.class, new String[] {"myParameterizedHashMap"});

        expression = "genericGetValue(myParameterizedHashMap)";
        resultExpression = "genericGetValue(myParameterizedHashMap)";
        check(expression, resultExpression, Double.class, new String[] {"myParameterizedHashMap"});

        expression = "genericArray(myVector)";
        resultExpression = "genericArray(myVector)";
        check(expression, resultExpression, new Double[0].getClass(), new String[] {"myVector"});
    }

    public void testVectorUnboxing() throws Exception {
        String expression = "genericArrayToSingle(myVector)";
        String resultExpression = "genericArrayToSingle(VectorConversions.nullSafeVectorToArray(myVector))";
        check(expression, resultExpression, Double.class, new String[] {"myVector"});

        expression = "genericArraysToSingle(myVector, myIntegerObjArray)";
        resultExpression =
                "genericArraysToSingle(VectorConversions.nullSafeVectorToArray(myVector), myIntegerObjArray)";
        check(expression, resultExpression, Integer.class, new String[] {"myVector", "myIntegerObjArray"});

        expression = "genericArraysToSingle(myIntegerObjArray, myVector)";
        resultExpression =
                "genericArraysToSingle(myIntegerObjArray, VectorConversions.nullSafeVectorToArray(myVector))";
        check(expression, resultExpression, Double.class, new String[] {"myVector", "myIntegerObjArray"});

        expression = "genericVector(myVector)";
        resultExpression = "genericVector(myVector)";
        check(expression, resultExpression, Double.class, new String[] {"myVector"});

        expression = "genericArrayToSingle(myObjectVector)";
        resultExpression = "genericArrayToSingle(VectorConversions.nullSafeVectorToArray(myObjectVector))";
        check(expression, resultExpression, Object.class, new String[] {"myObjectVector"});

        expression = "intArrayToInt(myIntVector)";
        resultExpression = "intArrayToInt(VectorConversions.nullSafeVectorToArray(myIntVector))";
        check(expression, resultExpression, int.class, new String[] {"myIntVector"});

        expression = "myIntVector==myIntVector";
        resultExpression =
                "eqArray(VectorConversions.nullSafeVectorToArray(myIntVector), VectorConversions.nullSafeVectorToArray(myIntVector))";
        check(expression, resultExpression, boolean[].class, new String[] {"myIntVector"});

        expression = "new String(myByteArray)";
        resultExpression = "new String(myByteArray)";
        check(expression, resultExpression, String.class, new String[] {"myByteArray"});

        expression = "new String(myByteVector)";
        resultExpression = "new String(VectorConversions.nullSafeVectorToArray(myByteVector))";
        check(expression, resultExpression, String.class, new String[] {"myByteVector"});
    }

    public void testVarArgsUnboxing() throws Exception {
        String expression = "testImplicitConversion_double(myInt)";
        String resultExpression = "testImplicitConversion_double(new double[] { doubleCast(myInt) })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myInt"});

        expression = "testImplicitConversion_double(myInt, myFloat)";
        resultExpression = "testImplicitConversion_double(new double[] { doubleCast(myInt), doubleCast(myFloat) })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myInt", "myFloat"});

        expression = "testImplicitConversion_double(myInt, myDouble, myLong, myInt, myDouble, myLong)";
        resultExpression =
                "testImplicitConversion_double(new double[] { doubleCast(myInt), myDouble, doubleCast(myLong), doubleCast(myInt), myDouble, doubleCast(myLong) })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDouble", "myInt", "myLong"});
    }

    public void testInnerClassesMethods() throws Exception {
        String expression = "myDummyClass.innerClassInstance.innerClassMethod()";
        String resultExpression = "myDummyClass.innerClassInstance.innerClassMethod()";
        check(expression, resultExpression, String.class, new String[] {"myDummyClass"});

        expression = "myDummyInnerClass.innerClassMethod()";
        resultExpression = "myDummyInnerClass.innerClassMethod()";
        check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});

        expression = "myDummyInnerClass.innerClassMethod()";
        resultExpression = "myDummyInnerClass.innerClassMethod()";
        check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});
    }

    public void testStaticNestedClassMethod() throws Exception {
        String expression = "LanguageParserDummyClass.StaticNestedClass.staticMethod()";
        String resultExpression = "LanguageParserDummyClass.StaticNestedClass.staticMethod()";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "myDummyStaticNestedClass.staticMethod()";
        resultExpression = "myDummyStaticNestedClass.staticMethod()";
        check(expression, resultExpression, String.class, new String[] {"myDummyStaticNestedClass"});

        expression = "myDummyStaticNestedClass.instanceMethod()";
        resultExpression = "myDummyStaticNestedClass.instanceMethod()";
        check(expression, resultExpression, String.class, new String[] {"myDummyStaticNestedClass"});
    }

    public void testInnerClasses() throws Exception {
        QueryLanguageParser.Result result =
                new QueryLanguageParser(
                        "io.deephaven.engine.table.impl.lang.TestQueryLanguageParser.InnerEnum.YEAH!=null", null,
                        null, staticImports, null, null).getResult();
        assertEquals("!isNull(io.deephaven.engine.table.impl.lang.TestQueryLanguageParser.InnerEnum.YEAH)",
                result.getConvertedExpression());
    }

    public void testComplexExpressions() throws Exception {
        String expression =
                "java.util.stream.Stream.of(new String[]{ `a`, `b`, `c`, myInt > 0 ? myString=Double.toString(myDouble) ? `1` : `2` : new LanguageParserDummyClass().toString() }).count()";
        String resultExpression =
                "java.util.stream.Stream.of(new String[] { \"a\", \"b\", \"c\", greater(myInt, 0) ? eq(myString, Double.toString(myDouble)) ? \"1\" : \"2\" : new LanguageParserDummyClass().toString() }).count()";
        check(expression, resultExpression, long.class, new String[] {"myDouble", "myInt", "myString"});

        expression = "myDummyClass.innerClassInstance.staticVar == 1_000_000L" +
                "? new int[] { java.util.stream.Stream.of(new String[] { `a`, `b`, `c`, myInt > 0 ? myString=Double.toString(myDouble) ? `1` : `2` : new LanguageParserDummyClass().toString() }).count() }"
                +
                ": myIntArray";
        resultExpression = "eq(myDummyClass.innerClassInstance.staticVar, 1_000_000L)" +
                " ? new int[] { java.util.stream.Stream.of(new String[] { \"a\", \"b\", \"c\", greater(myInt, 0) ? eq(myString, Double.toString(myDouble)) ? \"1\" : \"2\" : new LanguageParserDummyClass().toString() }).count() }"
                + " : myIntArray";
        check(expression, resultExpression, int[].class,
                new String[] {"myDouble", "myDummyClass", "myInt", "myIntArray", "myString"});

        // This comes from a strategy query:
        expression =
                "min( abs(ExampleQuantity), (int)round(min(ExampleQuantity2, (`String1`.equals(ExampleStr) ? max(-ExampleQuantity3,0d) : max(ExampleQuantity3,0d))/ExampleQuantity4)))";
        resultExpression =
                "min(abs(ExampleQuantity), intCast(round(min(ExampleQuantity2, divide((\"String1\".equals(ExampleStr) ? max(negate(ExampleQuantity3), 0d) : max(ExampleQuantity3, 0d)), ExampleQuantity4)))))";
        check(expression, resultExpression, int.class, new String[] {"ExampleQuantity", "ExampleQuantity2",
                "ExampleQuantity3", "ExampleQuantity4", "ExampleStr"});

        // There is a test for an erroneous version of this expression in testBadFieldAccess():
        expression =
                "io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.interpolate(myDoubleVector.toArray(),myDoubleVector.toArray(),new double[]{myDouble},io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.NestedEnum.ONE,false)[0]";
        resultExpression =
                "io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.interpolate(myDoubleVector.toArray(), myDoubleVector.toArray(), new double[] { myDouble }, io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.NestedEnum.ONE, false)[0]";
        check(expression, resultExpression, double.class, new String[] {"myDouble", "myDoubleVector"});

        // For good measure, same test as above w/ implicit array conversions:
        expression =
                "LanguageParserDummyClass.interpolate(myDoubleVector,myDoubleVector,new double[]{myDouble},LanguageParserDummyClass.NestedEnum.ONE,false)[0]";
        resultExpression =
                "LanguageParserDummyClass.interpolate(VectorConversions.nullSafeVectorToArray(myDoubleVector), VectorConversions.nullSafeVectorToArray(myDoubleVector), new double[] { myDouble }, LanguageParserDummyClass.NestedEnum.ONE, false)[0]";
        check(expression, resultExpression, double.class, new String[] {"myDouble", "myDoubleVector"});

        // @formatter:off
        // This following two cases are derived from CumulativeUtilTest.testCumMin, which caught a bug where a
        // primitive cast is an operand to a binary operator that's used in a method argument. Specifically, the problem
        // arises when:
        // - a method call argument is converted into a new expression, e.g. the BinaryExpr 'aDouble * myInt' into multiply(aDouble, myInt)
        // - that converted method call argument is pushed down into a new expression (e.g. wrapping varargs with explicit array initializer)
        // - the method call's arguments are replaced with .setArguments(theNewExpression)
        //
        // In this case, the BinaryExpr has the ArrayInitializerExpr as its parent, but the original method call still
        // has the BinaryExpr as an argument. So when .setArguments() is called with the ArrayCreationExpr, the
        // original MethodCallExpr will still see the BinaryExpr among its children and will (incorrectly) clear the
        // BinaryExpr's parent. This leaves the BinaryExpr without a reference to the ArrayInitializerExpr, which
        // will prevent the BinaryExpr from being replaced with a 'multiply()' method call.
        //
        // This problem could arise for both MethodCallExprs and ObjectCreationExprs. It is fixed by using DummyNodes
        // as placeholders when modifying argument expressions in QueryLanguageParser.convertParameters().
        // @formatter:on
        expression = "io.deephaven.function.Numeric.min(myDouble, myDouble*myDouble)";
        resultExpression = "io.deephaven.function.Numeric.min(new double[] { myDouble, multiply(myDouble, myDouble) })";
        check(expression, resultExpression, double.class, new String[] {"myDouble"});

        expression = "io.deephaven.function.Numeric.min(myDouble, (double)1*myInt)";
        resultExpression =
                "io.deephaven.function.Numeric.min(new double[] { myDouble, multiply(doubleCast(1), myInt) })";
        check(expression, resultExpression, double.class, new String[] {"myDouble", "myInt"});

        // another case of same bug (varargs widening primitive)
        expression = "testImplicitConversion_double(myFloat, myFloat*myFloat)";
        resultExpression =
                "testImplicitConversion_double(new double[] { doubleCast(myFloat), doubleCast(multiply(myFloat, myFloat)) })";
        check(expression, resultExpression, double[].class, new String[] {"myFloat"});

        // check another case (varargs unboxing)
        expression = "testImplicitConversion_boolean(myBooleanObj, myBooleanObj & myBooleanObj==null)";
        resultExpression =
                "testImplicitConversion_boolean(new boolean[] { myBooleanObj.booleanValue(), binaryAnd(myBooleanObj, isNull(myBooleanObj)).booleanValue() })";
        check(expression, resultExpression, boolean[].class, new String[] {"myBooleanObj"});

        // This comes from io.deephaven.engine.table.impl.QueryTableAjTest.testIds5293():
        expression =
                "(float)(myInt%14==0 ? null : myInt%10==0 ? 1.0F/0.0F: myInt%5==0 ? -1.0F/0.0F : (float) myIntObj*(Math.random()*2-1))";
        resultExpression =
                "floatCast((eq(remainder(myInt, 14), 0) ? NULL_DOUBLE : eq(remainder(myInt, 10), 0) ? divide(1.0F, 0.0F) : eq(remainder(myInt, 5), 0) ? divide(negate(1.0F), 0.0F) : multiply(floatCast(intCast(myIntObj)), (minus(multiply(Math.random(), 2), 1)))))";
        check(expression, resultExpression, float.class, new String[] {"myInt", "myIntObj"});
    }

    public void testUnsupportedOperators() throws Exception {
        String expression, resultExpression;

        try {
            expression = resultExpression = "myInt++";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt--";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "++myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "--myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt += myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt -= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt *= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt /= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt /= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt &= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt |= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt ^= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        // TODO: #3263 these are easy to support; just need to include in QueryLanguageFunctionGenerator
        try {
            expression = resultExpression = "myInt << myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt >> myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt >>> myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParser.QueryLanguageParseException ignored) {
        }
    }

    public void testIsWideningPrimitiveConversion() {
        {
            Require.eqFalse(isWideningPrimitiveConversion(byte.class, byte.class),
                    "isWideningPrimitiveConversion(byte.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(byte.class, char.class),
                    "isWideningPrimitiveConversion(byte.class, char.class)");

            Require.eqTrue(isWideningPrimitiveConversion(byte.class, short.class),
                    "isWideningPrimitiveConversion(byte.class, short.class)");
            Require.eqTrue(isWideningPrimitiveConversion(byte.class, int.class),
                    "isWideningPrimitiveConversion(byte.class, int.class)");
            Require.eqTrue(isWideningPrimitiveConversion(byte.class, long.class),
                    "isWideningPrimitiveConversion(byte.class, long.class)");
            Require.eqTrue(isWideningPrimitiveConversion(byte.class, float.class),
                    "isWideningPrimitiveConversion(byte.class, float.class)");
            Require.eqTrue(isWideningPrimitiveConversion(byte.class, double.class),
                    "isWideningPrimitiveConversion(byte.class, double.class)");
        }


        {
            Require.eqFalse(isWideningPrimitiveConversion(short.class, byte.class),
                    "isWideningPrimitiveConversion(short.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(short.class, short.class),
                    "isWideningPrimitiveConversion(short.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(short.class, char.class),
                    "isWideningPrimitiveConversion(short.class, char.class)");

            Require.eqTrue(isWideningPrimitiveConversion(short.class, int.class),
                    "isWideningPrimitiveConversion(short.class, int.class)");
            Require.eqTrue(isWideningPrimitiveConversion(short.class, long.class),
                    "isWideningPrimitiveConversion(short.class, long.class)");
            Require.eqTrue(isWideningPrimitiveConversion(short.class, float.class),
                    "isWideningPrimitiveConversion(short.class, float.class)");
            Require.eqTrue(isWideningPrimitiveConversion(short.class, double.class),
                    "isWideningPrimitiveConversion(short.class, double.class)");
        }

        {
            Require.eqFalse(isWideningPrimitiveConversion(char.class, byte.class),
                    "isWideningPrimitiveConversion(char.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(char.class, short.class),
                    "isWideningPrimitiveConversion(char.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(char.class, char.class),
                    "isWideningPrimitiveConversion(char.class, char.class)");

            Require.eqTrue(isWideningPrimitiveConversion(char.class, int.class),
                    "isWideningPrimitiveConversion(char.class, int.class)");
            Require.eqTrue(isWideningPrimitiveConversion(char.class, long.class),
                    "isWideningPrimitiveConversion(char.class, long.class)");
            Require.eqTrue(isWideningPrimitiveConversion(char.class, float.class),
                    "isWideningPrimitiveConversion(char.class, float.class)");
            Require.eqTrue(isWideningPrimitiveConversion(char.class, double.class),
                    "isWideningPrimitiveConversion(char.class, double.class)");
        }

        {
            Require.eqFalse(isWideningPrimitiveConversion(int.class, byte.class),
                    "isWideningPrimitiveConversion(int.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(int.class, short.class),
                    "isWideningPrimitiveConversion(int.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(int.class, char.class),
                    "isWideningPrimitiveConversion(int.class, char.class)");
            Require.eqFalse(isWideningPrimitiveConversion(int.class, int.class),
                    "isWideningPrimitiveConversion(int.class, int.class)");

            Require.eqTrue(isWideningPrimitiveConversion(int.class, long.class),
                    "isWideningPrimitiveConversion(int.class, long.class)");
            Require.eqTrue(isWideningPrimitiveConversion(int.class, float.class),
                    "isWideningPrimitiveConversion(int.class, float.class)");
            Require.eqTrue(isWideningPrimitiveConversion(int.class, double.class),
                    "isWideningPrimitiveConversion(int.class, double.class)");
        }

        {
            Require.eqFalse(isWideningPrimitiveConversion(long.class, byte.class),
                    "isWideningPrimitiveConversion(long.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(long.class, short.class),
                    "isWideningPrimitiveConversion(long.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(long.class, char.class),
                    "isWideningPrimitiveConversion(long.class, char.class)");
            Require.eqFalse(isWideningPrimitiveConversion(long.class, int.class),
                    "isWideningPrimitiveConversion(long.class, int.class)");
            Require.eqFalse(isWideningPrimitiveConversion(long.class, long.class),
                    "isWideningPrimitiveConversion(long.class, long.class)");


            Require.eqTrue(isWideningPrimitiveConversion(long.class, float.class),
                    "isWideningPrimitiveConversion(long.class, float.class)");
            Require.eqTrue(isWideningPrimitiveConversion(long.class, double.class),
                    "isWideningPrimitiveConversion(long.class, double.class)");
        }


        {
            Require.eqFalse(isWideningPrimitiveConversion(float.class, byte.class),
                    "isWideningPrimitiveConversion(float.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(float.class, short.class),
                    "isWideningPrimitiveConversion(float.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(float.class, char.class),
                    "isWideningPrimitiveConversion(float.class, char.class)");
            Require.eqFalse(isWideningPrimitiveConversion(float.class, int.class),
                    "isWideningPrimitiveConversion(float.class, int.class)");
            Require.eqFalse(isWideningPrimitiveConversion(float.class, long.class),
                    "isWideningPrimitiveConversion(float.class, long.class)");

            Require.eqTrue(isWideningPrimitiveConversion(float.class, double.class),
                    "isWideningPrimitiveConversion(float.class, double.class)");
        }

        {
            Require.eqFalse(isWideningPrimitiveConversion(double.class, byte.class),
                    "isWideningPrimitiveConversion(double.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(double.class, short.class),
                    "isWideningPrimitiveConversion(double.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(double.class, char.class),
                    "isWideningPrimitiveConversion(double.class, char.class)");
            Require.eqFalse(isWideningPrimitiveConversion(double.class, int.class),
                    "isWideningPrimitiveConversion(double.class, int.class)");
            Require.eqFalse(isWideningPrimitiveConversion(double.class, long.class),
                    "isWideningPrimitiveConversion(double.class, long.class)");
            Require.eqFalse(isWideningPrimitiveConversion(double.class, float.class),
                    "isWideningPrimitiveConversion(double.class, float.class)");
            Require.eqFalse(isWideningPrimitiveConversion(double.class, double.class),
                    "isWideningPrimitiveConversion(double.class, long.class)");
        }

        {
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, byte.class),
                    "isWideningPrimitiveConversion(boolean.class, byte.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, short.class),
                    "isWideningPrimitiveConversion(boolean.class, short.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, char.class),
                    "isWideningPrimitiveConversion(boolean.class, char.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, int.class),
                    "isWideningPrimitiveConversion(boolean.class, int.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, long.class),
                    "isWideningPrimitiveConversion(boolean.class, long.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, float.class),
                    "isWideningPrimitiveConversion(boolean.class, float.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, double.class),
                    "isWideningPrimitiveConversion(boolean.class, double.class)");
            Require.eqFalse(isWideningPrimitiveConversion(boolean.class, boolean.class),
                    "isWideningPrimitiveConversion(boolean.class, boolean.class)");
        }
    }

    @SuppressWarnings("ClassGetClass") // class.getClass() is the purpose of this test
    public void testClassExpr() throws Exception {
        String expression = "Integer.class";
        String resultExpression = "Integer.class";
        check(expression, resultExpression, Integer.class.getClass(), new String[] {});

        expression = "String.class";
        resultExpression = "String.class";
        check(expression, resultExpression, String.class.getClass(), new String[] {});

        expression = "LanguageParserDummyClass.class";
        resultExpression = "LanguageParserDummyClass.class";
        check(expression, resultExpression, LanguageParserDummyClass.class.getClass(), new String[] {});

        expression = "LanguageParserDummyClass.InnerClass.class";
        resultExpression = "LanguageParserDummyClass.InnerClass.class";
        check(expression, resultExpression, LanguageParserDummyClass.InnerClass.class.getClass(), new String[] {});

        expression = "LanguageParserDummyInterface.class";
        resultExpression = "LanguageParserDummyInterface.class";
        check(expression, resultExpression, LanguageParserDummyInterface.class.getClass(), new String[] {});
    }

    public void testClassImports() throws Exception {
        String expression = "LanguageParserDummyClass.value";
        String resultExpression = "LanguageParserDummyClass.value";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.value";
        resultExpression = "io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.value";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "(LanguageParserDummyClass) myObject";
        resultExpression = "(LanguageParserDummyClass) myObject";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {"myObject"});

        expression = "(io.deephaven.engine.table.impl.lang.LanguageParserDummyClass) myObject";
        resultExpression = "(io.deephaven.engine.table.impl.lang.LanguageParserDummyClass) myObject";
        check(expression, resultExpression, LanguageParserDummyClass.class, new String[] {"myObject"});
    }


    public void testStaticImports() throws Exception {
        // test using imports from io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.StaticNestedClass
        String expression = "staticVar";
        String resultExpression = "staticVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "staticMethod()";
        resultExpression = "staticMethod()";
        check(expression, resultExpression, String.class, new String[] {});
        expression = "(StaticDoubleNestedClass) myObject";
        resultExpression = "(StaticDoubleNestedClass) myObject";
        check(expression, resultExpression, LanguageParserDummyClass.StaticNestedClass.StaticDoubleNestedClass.class,
                new String[] {"myObject"});

        expression =
                "(io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.StaticNestedClass.StaticDoubleNestedClass) myObject";
        resultExpression =
                "(io.deephaven.engine.table.impl.lang.LanguageParserDummyClass.StaticNestedClass.StaticDoubleNestedClass) myObject";
        check(expression, resultExpression, LanguageParserDummyClass.StaticNestedClass.StaticDoubleNestedClass.class,
                new String[] {"myObject"});

        // make sure the non-static members are not treated as imported:
        try {
            expression = "instanceVar";
            resultExpression = "instanceVar";
            check(expression, resultExpression, String.class, new String[] {});
            fail("Should have thrown an exception!");
        } catch (QueryLanguageParseException ignored) {
        }

        try {
            expression = "instanceMethod()";
            resultExpression = "instanceMethod()";
            check(expression, resultExpression, String.class, new String[] {});
            fail("Should have thrown an exception!");
        } catch (QueryLanguageParseException ignored) {
        }

        // make sure static imports that *aren't* classes throw exceptions:
        try {
            expression = "(staticVar) myObject";
            resultExpression = "(staticVar) myObject";
            check(expression, resultExpression, String.class, new String[] {"myObject"});
            fail("Should have thrown an exception!");
        } catch (QueryLanguageParseException ignored) {
        }
    }


    public void testGenericMethodCall() throws Exception {
        String expression = "LanguageParserDummyClass.typedRefWithCapture(`hello`)";
        String resultExpression = "LanguageParserDummyClass.typedRefWithCapture(\"hello\")";
        check(expression, resultExpression, String.class, new String[] {});

        // Call a generic method with explicit type arguments to ensure that the type arguments are printed.
        // The type arguments (i.e., '<Object>'/'<Object, Object>') will be included in the output but not processed
        // by the parser (that is why they must be set to 'Object' in this test).
        expression = "LanguageParserDummyClass.<Object>typedRef()";
        resultExpression = "LanguageParserDummyClass.<Object>typedRef()";
        check(expression, resultExpression, Object.class, new String[] {});

        expression = "LanguageParserDummyClass.<Object, Object>typedRefTwoTypes()";
        resultExpression = "LanguageParserDummyClass.<Object, Object>typedRefTwoTypes()";
        check(expression, resultExpression, Pair.class, new String[] {});

        // // Call generic method with explicit type arguments:
        // expression="LanguageParserDummyClass.<String>typedRef()";
        // resultExpression="LanguageParserDummyClass.<String>typedRef()";
        // check(expression, resultExpression, String.class, new String[]{});

        // Same method, no type args:
        expression = "LanguageParserDummyClass.typedRef()";
        resultExpression = "LanguageParserDummyClass.typedRef()";
        check(expression, resultExpression, Object.class, new String[] {});

        // // Test when type is bounded:
        // expression="LanguageParserDummyClass.<String>typedRefBounded()";
        // resultExpression="LanguageParserDummyClass.<String>typedRefBounded()";
        // check(expression, resultExpression, String.class, new String[]{});

        expression = "LanguageParserDummyClass.typedRefBounded()";
        resultExpression = "LanguageParserDummyClass.typedRefBounded()";
        check(expression, resultExpression, CharSequence.class, new String[] {});
    }

    // public void testGenericConstructor() throws Exception {
    // String expression="genericGetKey(new HashMap<String, Integer>())";
    // String resultExpression="genericGetKey(new HashMap<String, Integer>())";
    // check(expression, resultExpression, String.class, new String[]{});
    //
    // expression="genericGetValue(new HashMap<String, Integer>())";
    // resultExpression="genericGetValue(new HashMap<String, Integer>())";
    // check(expression, resultExpression, Integer.class, new String[]{});
    //
    // expression="new HashMap<String, String>().get(\"test\")";
    // resultExpression="new HashMap<String, String>().get(\"test\")";
    // check(expression, resultExpression, String.class, new String[]{});
    //
    // expression="new HashMap<String, Integer>().get(\"test\")";
    // resultExpression="new HashMap<String, Integer>().get(\"test\")";
    // check(expression, resultExpression, Integer.class, new String[]{});
    //
    // /*
    // This one fails because WildcardType is not supported (because we have not implemented `visit(WildcardType n,
    // StringBuilder printer)`)
    // expression="new HashMap<String, ? extends Number>().get(\"test\")";
    // resultExpression="new HashMap<String, ? extends Number>().get(\"test\")";
    // check(expression, resultExpression, Number.class, new String[]{});
    // */
    // }

    public void testGenericReturnTypeOfScopeVar() throws Exception {
        String expression = "myParameterizedHashMap.get(0)";
        String resultExpression = "myParameterizedHashMap.get(0)";
        check(expression, resultExpression, Double.class, new String[] {"myParameterizedHashMap"});
    }

    public void testGenericClass() throws Exception {
        // String expression = "myParameterizedClass.var";
        // String resultExpression = "myParameterizedClass.var";
        // check(expression, resultExpression, String.class, new String[] {"myParameterizedClass"});

        String expression = "myParameterizedClass.getVar()";
        String resultExpression = "myParameterizedClass.getVar()";
        check(expression, resultExpression, String.class, new String[] {"myParameterizedClass"});

        expression = "myParameterizedClass.getArr()";
        resultExpression = "myParameterizedClass.getArr()";
        check(expression, resultExpression, new String[0].getClass(), new String[] {"myParameterizedClass"});

        // expression = "myArrayParameterizedClass.var";
        // resultExpression = "myArrayParameterizedClass.var";
        // check(expression, resultExpression, new String[0].getClass(), new String[] {"myArrayParameterizedClass"});

        expression = "myArrayParameterizedClass.getVar()";
        resultExpression = "myArrayParameterizedClass.getVar()";
        check(expression, resultExpression, new String[0].getClass(), new String[] {"myArrayParameterizedClass"});

        expression = "myArrayParameterizedClass.getArr()";
        resultExpression = "myArrayParameterizedClass.getArr()";
        check(expression, resultExpression, new String[0][0].getClass(), new String[] {"myArrayParameterizedClass"});
    }

//@formatter:off
//     public void testGenericNested() throws Exception {
//         String expression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().var";
//         String resultExpression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().var";
//         check(expression, resultExpression, String.class, new String[]{});
//
//         expression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().getVar()";
//         resultExpression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().getVar()";
//         check(expression, resultExpression, String.class, new String[]{});
//
//         expression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().varOfOuterType";
//         resultExpression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().varOfOuterType";
//         check(expression, resultExpression, String.class, new String[]{});
//
//         expression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().getVarOfOuterType()";
//         resultExpression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().getVarOfOuterType()";
//         check(expression, resultExpression, String.class, new String[]{});
//
//         expression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().varOfInnerType";
//         resultExpression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().varOfInnerType";
//         check(expression, resultExpression, Double.class, new String[]{});
//
//         expression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().getVarOfInnerType()";
//         resultExpression="new LanguageParserDummyClass.StaticNestedGenericClass<String>().<Double>getInnerInstance().getVarOfInnerType()";
//         check(expression, resultExpression, Double.class, new String[]{});
//     }
//@formatter:on

    public void testDhqlIsAssignableFrom() {
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(String.class, String.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(Object.class, String.class));
        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(Integer.class, String.class));

        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(int.class, int.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(Integer.class, Integer.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(int.class, Integer.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(Integer.class, int.class));

        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(double[].class, double[].class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(DoubleVector.class, DoubleVector.class));
        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(double[].class, DoubleVector.class));
        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(DoubleVector.class, double[].class));

        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(char.class, int.class));
        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(byte.class, int.class));
        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(short.class, int.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(float.class, int.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(double.class, int.class));
        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(long.class, int.class));

        assertTrue(QueryLanguageParser.dhqlIsAssignableFrom(Vector.class, new ObjectVectorDirect<String>().getClass()));
        assertFalse(QueryLanguageParser.dhqlIsAssignableFrom(double[].class, Vector.class));
    }

    public void testInvalidExpr() throws Exception {
        String expression = "1+";
        expectFailure(expression, int.class);

        expression = "(1+2))";
        expectFailure(expression, int.class);

        expression = "new int[]{1, 2, 3},";
        expectFailure(expression, int.class);

        expression = "'a', 'b'";
        expectFailure(expression, int.class);

        expression = "('a'+'b'";
        expectFailure(expression, int.class);

        expression = "plus('1', '2') * '1' -- = 3;"; // we are parsing expressions, not statements.
        expectFailure(expression, int.class);

        expression = "plus(1, 2) minus(2, 1) plus(0)";
        expectFailure(expression, int.class);

        expression = "plus(1, 2))";
        expectFailure(expression, int.class);

        // this was getting picked up as invalid, so we're ensuring here that it is not an error.
        expression = "23 >= plus(System.currentTimeMillis(), 12)";
        check(expression, "greaterEquals(23, plus(System.currentTimeMillis(), 12))", boolean.class, new String[0]);
    }

    public static class TestGenericResult1 {
    }
    public static class TestGenericResult2 {
    }
    public static class TestGenericResult3 {
    }
    public static class TestGenericResult4 {
    }
    public static class TestGenericResult5 {
    }
    public interface TestGenericInterfaceSuper<T> {
        default T getFromInterfaceSuper() {
            return null;
        }
    }
    public interface TestGenericInterfaceForSuper<T> {
        default T getFromInterfaceForSuper() {
            return null;
        }
    }
    public interface TestGenericInterfaceSub<T> extends TestGenericInterfaceSuper<TestGenericResult1> {
        default T getFromInterfaceSub() {
            return null;
        }
    }
    public interface TestGenericInterfaceSibling<T> {
        default T getFromInterfaceSibling() {
            return null;
        }
    }
    public static abstract class TestGenericSuper<T> implements TestGenericInterfaceForSuper<TestGenericResult2> {
        public T getFromSuper() {
            return null;
        }
    }
    public static class TestGenericSub extends TestGenericSuper<TestGenericResult3>
            implements TestGenericInterfaceSub<TestGenericResult4>, TestGenericInterfaceSibling<TestGenericResult5> {
    }

    public void testGenericReturnTypeResolution() throws Exception {
        final String[] resultVarsUsed = new String[] {"genericSub"};

        String expression = "genericSub.getFromInterfaceSuper()";
        check(expression, expression, TestGenericResult1.class, resultVarsUsed);
        expression = "genericSub.getFromInterfaceForSuper()";
        check(expression, expression, TestGenericResult2.class, resultVarsUsed);
        expression = "genericSub.getFromSuper()";
        check(expression, expression, TestGenericResult3.class, resultVarsUsed);
        expression = "genericSub.getFromInterfaceSub()";
        check(expression, expression, TestGenericResult4.class, resultVarsUsed);
        expression = "genericSub.getFromInterfaceSibling()";
        check(expression, expression, TestGenericResult5.class, resultVarsUsed);
    }

    @SuppressWarnings("SameParameterValue")
    private void expectFailure(String expression, Class<?> resultType) throws Exception {
        try {
            check(expression, expression, resultType, new String[0]);
            fail("Should have thrown a QueryLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParseException expected) {
        }
    }

    private void check(String expression, String resultExpression, Class<?> resultType, String[] resultVarsUsed)
            throws Exception {
        check(expression, resultExpression, resultType, resultVarsUsed, true);
    }

    private void check(String expression, String resultExpression, Class<?> resultType, String[] resultVarsUsed,
            boolean verifyIdempotence)
            throws Exception {
        final Map<String, Object> possibleParams;
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        if (!(queryScope instanceof PoisonedQueryScope)) {
            possibleParams = QueryCompilerRequestProcessor.newFormulaImportsSupplier().get().getQueryScopeVariables();
        } else {
            possibleParams = null;
        }

        final QueryLanguageParser.Result result =
                new QueryLanguageParser(expression, packageImports, classImports, staticImports,
                        variables, variableParameterizedTypes, possibleParams, null,
                        true,
                        verifyIdempotence,
                        PyCallableWrapperDummyImpl.class.getName(), null).getResult();

        assertEquals(resultType, result.getType());
        assertEquals(resultExpression, result.getConvertedExpression());

        String[] variablesUsed = result.getVariablesUsed().toArray(new String[0]);

        Arrays.sort(resultVarsUsed);
        Arrays.sort(variablesUsed);
        assertEquals(resultVarsUsed, variablesUsed);
    }

    @SuppressWarnings("InnerClassMayBeStatic")
    class TestClass implements Comparable<TestClass> {
        public int compareTo(@NotNull TestClass o) {
            return 0;
        }

        public final int var = 0;
    }

    public static int plus(TestClass testClass, int i) {
        return -1;
    }

    public static char multiply(TestClass testClass, double d) {
        return 'a';
    }

    public static String minus(TestClass testClass, char c) {
        return "";
    }

    public static boolean less(TestClass testClass, int i) {
        return true;
    }

    public static boolean greater(TestClass testClass, int i) {
        return true;
    }

    public static boolean lessEquals(TestClass testClass, int i) {
        return true;
    }

    public static boolean greaterEquals(TestClass testClass, int i) {
        return true;
    }

    public static boolean[] testImplicitConversion_boolean(boolean... booleans) {
        return booleans;
    }

    public static char[] testImplicitConversion_char(char... c) {
        return c;
    }

    public static byte[] testImplicitConversion_byte(byte... b) {
        return b;
    }

    public static short[] testImplicitConversion_short(short... s) {
        return s;
    }

    public static int[] testImplicitConversion_int(int... i) {
        return i;
    }

    public static float[] testImplicitConversion_float(float... f) {
        return f;
    }

    public static long[] testImplicitConversion_long(long... l) {
        return l;
    }

    public static double[] testImplicitConversion_double(double... d) {
        return d;
    }

    public static Object[] testImplicitConversion_Object(Object... o) {
        return o;
    }

    public static Object[] testImplicitConversion4(double d, Object... o) {
        return o;
    }

    public static Object[] testImplicitConversion5(Vector... o) {
        return o;
    }

    public static Number[] testImplicitConversionArraySuper(Number... o) {
        return o;
    }

    public static Number[][] testImplicitConversionNestedArraySuper(Number[]... o) {
        return o;
    }

    public static double testVarArgs(final double... d) {
        return d[0];
    }

    public static double[] testVarArgs(int a, char c, double... d) {
        return d;
    }

    public static char[] testVarArgs(double d, int i, char... c) {
        return c;
    }

    public static float[] testVarArgs(int i, char c, float... f) {
        return f;
    }

    public static IntVector[] testVarArgsVector(final IntVector... a) {
        return a;
    }

    public static <T> T genericSingleToSingle(T t) {
        return null;
    }

    public static <T> T[] genericArrayToArray(T t[]) {
        return null;
    }

    public static <T> T genericArrayToSingle(T t[]) {
        return null;
    }

    public static <T> T[] genericSingleToArray(T t) {
        return null;
    }

    public static <T> T[][] genericSingleToDoubleArray(T t) {
        return null;
    }

    public static <T> T genericDoubleArrayToSingle(T t[][]) {
        return null;
    }

    public static <K, V> K genericGetKey(HashMap<K, V> map) {
        return null;
    }

    public static <K, V> V genericGetValue(HashMap<K, V> map) {
        return null;
    }

    public static <A, B> B genericArraysToSingle(A a[], B b[]) {
        return null;
    }

    public static <T> T genericVector(ObjectVector<T> vector) {
        return null;
    }

    public static <T> T[] genericArray(ObjectVector<T> vector) {
        return null;
    }

    public static int intArrayToInt(int i[]) {
        return -1;
    }

    public static Boolean booleanArrayToBoolean(Boolean bools[]) {
        return true;
    }

    public enum InnerEnum {
        YEAH
    }
}

