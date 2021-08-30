/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.lang;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.db.tables.lang.DBLanguageParser.QueryLanguageParseException;
import io.deephaven.db.tables.utils.*;
import io.deephaven.utils.test.PropertySaver;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import groovy.lang.Closure;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import java.awt.*;
import java.util.*;
import java.util.List;

import static io.deephaven.db.tables.lang.DBLanguageParser.isWideningPrimitiveConversion;

@SuppressWarnings("InstantiatingObjectToGetClassObject")
public class TestDBLanguageParser extends BaseArrayTestCase {

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
        Assert.equals(tablePackage.getName(), "tablePackage.getName()", "io.deephaven.db.tables");
        packageImports.add(tablePackage);

        classImports = new HashSet<>();
        classImports.add(Color.class);
        classImports.add(ArrayUtils.class);
        classImports.add(HashSet.class);
        classImports.add(DbArrayBase.class);
        classImports.add(DBLanguageParserDummyClass.class);
        classImports.add(DBLanguageParserDummyClass.SubclassOfDBLanguageParserDummyClass.class);
        classImports.add(DBLanguageParserDummyEnum.class);
        classImports.add(DBLanguageParserDummyInterface.class);

        staticImports = new HashSet<>();
        staticImports.add(DBLanguageFunctionUtil.class);
        staticImports.add(Math.class);
        staticImports.add(QueryConstants.class);
        staticImports.add(TestDBLanguageParser.class);

        variables = new HashMap<>();
        variables.put("myByte", byte.class);
        variables.put("myShort", short.class);
        variables.put("myChar", char.class);
        variables.put("myInt", int.class);
        variables.put("myOtherInt", int.class);
        variables.put("myLong", long.class);
        variables.put("myFloat", float.class);
        variables.put("myDouble", double.class);
        variables.put("myBoolean", boolean.class);
        variables.put("myString", String.class);
        variables.put("myCharSequence", CharSequence.class);
        variables.put("myObject", Object.class);
        variables.put("myTestClass", TestClass.class);
        variables.put("myIntArray", new int[0].getClass());
        variables.put("myDoubleArray", new double[0].getClass());
        variables.put("myCharArray", new char[0].getClass());
        variables.put("myTestClassArray", new TestClass[0].getClass());
        variables.put("myDoubleObjArray", new Double[0].getClass());
        variables.put("myIntegerObjArray", new Integer[0].getClass());
        variables.put("myByteArray", new byte[0].getClass());
        variables.put("myByteObj", Byte.class);
        variables.put("myShortObj", Short.class);
        variables.put("myCharObj", Character.class);
        variables.put("myIntObj", Integer.class);
        variables.put("myLongObj", Long.class);
        variables.put("myFloatObj", Float.class);
        variables.put("myDoubleObj", Double.class);
        variables.put("myBooleanObj", Boolean.class);
        variables.put("myArrayList", ArrayList.class);
        variables.put("myHashMap", HashMap.class);
        variables.put("myDBArray", DbArray.class);
        variables.put("myEnumValue", DBLanguageParserDummyEnum.class);
        variables.put("myObjectDBArray", DbArray.class);
        variables.put("myIntDBArray", DbIntArray.class);
        variables.put("myByteDBArray", DbByteArray.class);
        variables.put("myDoubleDBArray", DbDoubleArray.class);
        // noinspection deprecation
        variables.put("myBooleanDBArray", DbBooleanArray.class);
        variables.put("myDummyClass", DBLanguageParserDummyClass.class);
        variables.put("myDummyInnerClass", DBLanguageParserDummyClass.InnerClass.class);
        variables.put("myClosure", Closure.class);

        variables.put("myTable", Table.class);

        variables.put("ExampleQuantity", int.class);
        variables.put("ExampleQuantity2", double.class);
        variables.put("ExampleQuantity3", double.class);
        variables.put("ExampleQuantity4", double.class);
        variables.put("ExampleStr", String.class);

        variableParameterizedTypes = new HashMap<>();
        variableParameterizedTypes.put("myHashMap", new Class[] {Integer.class, Double.class});
        variableParameterizedTypes.put("myDBArray", new Class[] {Double.class});
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

        expression = "1.0+2L+3*4";
        resultExpression = "plus(plus(1.0, 2L), multiply(3, 4))";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "1==1";
        resultExpression = "eq(1, 1)";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "'g'=='g'";
        resultExpression = "eq('g', 'g')";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "false | true";
        resultExpression = "binaryOr(false, true)";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "1>1+2*3/4 || true";
        resultExpression = "greater(1, plus(1, divide(multiply(2, 3), 4)))||true";
        check(expression, resultExpression, boolean.class, new String[] {});

        expression = "(1>1+2*3/4) | true";
        resultExpression = "binaryOr((greater(1, plus(1, divide(multiply(2, 3), 4)))), true)";
        check(expression, resultExpression, Boolean.class, new String[] {});

        expression = "1+\"test\"";
        resultExpression = "1+\"test\"";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "1==1 ? 1+1 : 1.0";
        resultExpression = "eq(1, 1) ? plus(1, 1) : 1.0";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "(double)1";
        resultExpression = "doubleCast(1)";
        check(expression, resultExpression, double.class, new String[] {});

        expression = "(double[])myObject";
        resultExpression = "(double[])myObject";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myObject"});

        expression = "(double[][])myObject";
        resultExpression = "(double[][])myObject";
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
        resultExpression = "not(false)";
        check(expression, resultExpression, Boolean.class, new String[] {});
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
                DBLanguageParser.convertBackticks("`hello`"),
                "convertBackticks(\"`hello`\")",
                "\"hello\"");

        Require.equals(
                DBLanguageParser.convertBackticks("`'`"),
                "convertBackticks(\"`'`\")",
                "\"'\"");

        Require.equals(
                DBLanguageParser.convertBackticks("`\"`"),
                "convertBackticks(\"`\\\"`\")",
                "\"\\\"\"");

        Require.equals(
                DBLanguageParser.convertBackticks("\"`\""),
                "convertBackticks(\"\\\"`\\\"\")",
                "\"`\"");

        Require.equals(
                DBLanguageParser.convertBackticks("`'\\\"'`"),
                "convertBackticks(\"`'\\\\\\\"'`\")",
                "\"'\\\"'\"");

        Require.equals(
                DBLanguageParser.convertBackticks("\"`abc`\""),
                "convertBackticks(\"\\\"`abc`\\\"\")",
                "\"`abc`\"");

        Require.equals(
                DBLanguageParser.convertBackticks("\"`'abc`'\""),
                "convertBackticks(\"\\\"`'abc`'\\\"\")",
                "\"`'abc`'\"");

        Require.equals(
                DBLanguageParser.convertBackticks("\"'`\""),
                "convertBackticks(\"\\\"'`\\\"\")",
                "\"'`\"");

        Require.equals(
                DBLanguageParser.convertBackticks("`abc ` + \"def\" + \"`hij`\" + '`' + `'`"),
                "convertBackticks(\"`abc ` + \\\"def\\\" + \\\"`hij`\\\" + '`' + `'`\")",
                "\"abc \" + \"def\" + \"`hij`\" + '`' + \"'\"");

        // test each type of quote, escaped and contained within itself
        Require.equals(
                DBLanguageParser.convertBackticks("\"\\\"\""),
                "convertBackticks(\"\\\"\\\\\\\"\\\"\")",
                "\"\\\"\"");
        Require.equals(
                DBLanguageParser.convertBackticks("`\\``"),
                "convertBackticks(\"`\\\\``\")",
                "\"\\`\"");
        Require.equals(
                DBLanguageParser.convertBackticks("'\\''"),
                "convertBackticks(\"'\\\\''\")",
                "'\\''");

        // test tick and double quote both escaped within a string
        Require.equals(
                DBLanguageParser.convertBackticks("`\"\\``"),
                "convertBackticks(\"`\\\"\\\\``\")",
                "\"\\\"\\`\"");
        // here ` is unescaped, since it is within "s
        Require.equals(
                DBLanguageParser.convertBackticks("\"\\\"`\""),
                "convertBackticks(\"\\\"\\\\\\\"`\\\"\")",
                "\"\\\"`\"");

        // confirm that standard java escaping tools are sufficient to correctly escape strings for the DBLangParser
        Require.equals(
                DBLanguageParser.convertBackticks("\"" + StringEscapeUtils.escapeJava("`\"'\\") + "\""),
                "convertBackticks(escapeJava(\"`\\\"'\\\\\"))",
                "\"`\\\"'\\\\\"");
    }

    public void testConvertSingleEquals() {
        Require.equals(
                DBLanguageParser.convertSingleEquals("a=b"),
                "convertSingleEquals(\"a=b\")",
                "a==b");

        Require.equals(
                DBLanguageParser.convertSingleEquals("a=b=c==d=e==f"),
                "convertSingleEquals(\"a=b=c==d=e==f\")",
                "a==b==c==d==e==f");

        Require.equals(
                DBLanguageParser.convertSingleEquals("'='"),
                "convertSingleEquals(\"'='\")",
                "'='");

        Require.equals(
                DBLanguageParser.convertSingleEquals("'='='='"),
                "convertSingleEquals(\"'='='='\")",
                "'='=='='");

        Require.equals(
                DBLanguageParser.convertSingleEquals("'='='='=='='"),
                "convertSingleEquals(\"'='='='=='='\")",
                "'='=='='=='='");

        Require.equals(
                DBLanguageParser.convertSingleEquals("a='='=b"),
                "convertSingleEquals(\"a='='=b\")",
                "a=='='==b");

        Require.equals(
                DBLanguageParser.convertSingleEquals("\"a=b\""),
                "convertSingleEquals(\"a=b\")",
                "\"a=b\"");

        Require.equals(
                DBLanguageParser.convertSingleEquals("\"a=b'\"='='"),
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
        resultExpression = "(CharSequence)myString";
        check(expression, resultExpression, CharSequence.class, new String[] {"myString"});

        expression = "(String)(CharSequence)myString";
        resultExpression = "(String)((CharSequence)myString)";
        check(expression, resultExpression, String.class, new String[] {"myString"});

        expression = "(Double)myDouble";
        resultExpression = "(Double)myDouble";
        check(expression, resultExpression, Double.class, new String[] {"myDouble"});

        expression = "(Double)(double)myInt";
        resultExpression = "(Double)(doubleCast(myInt))";
        check(expression, resultExpression, Double.class, new String[] {"myInt"});

        expression = "(double)(int)myIntObj";
        resultExpression = "doubleCast(intCast(myIntObj))";
        check(expression, resultExpression, double.class, new String[] {"myIntObj"});

        expression = "(double)myIntObj"; // requires separate casts for unboxing & widening (see notes at
                                         // testBoxedToPrimitiveCasts, or JLS)
        resultExpression = "doubleCast(intCast(myIntObj))";
        check(expression, resultExpression, double.class, new String[] {"myIntObj"});

        expression = "(short)(double)(Double)(double)myInt";
        resultExpression = "shortCast(doubleCast((Double)(doubleCast(myInt))))";
        check(expression, resultExpression, short.class, new String[] {"myInt"});

        // TOOD: Test some invalid casts?

        try {
            // Test invalid boxing
            expression = "(Double)myInt";
            resultExpression = "(Double)myInt";
            check(expression, resultExpression, Double.class, new String[] {"myInt"});
            fail("Should have throw a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
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
                expression = '(' + targetType.first + ')' + literal.first; // e.g. "(int)42"
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
                    fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
                } catch (DBLanguageParser.QueryLanguageParseException ignored) {
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
                    fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
                } catch (DBLanguageParser.QueryLanguageParseException ignored) {
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed testing cast of " + literal.second.getName() + " to boolean", ex);
            }
        }

        // Test the identity conversion for booleans
        expression = resultExpression = "(boolean)true"; // e.g. "(int)true"
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
                expression = '(' + targetType.first + ')' + var.first; // e.g. "(int)myDouble"
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
                    fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
                } catch (DBLanguageParser.QueryLanguageParseException ignored) {
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
                    fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
                } catch (DBLanguageParser.QueryLanguageParseException ignored) {
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed testing cast of " + var.second.getName() + " to boolean", ex);
            }
        }

        // Test the identity conversion for booleans
        expression = resultExpression = "(boolean)true"; // e.g. "(int)true"
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

                expression = '(' + primitiveTypeName + ")" + boxedTypeTestVarName;
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
                            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
                        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
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
            expression = '(' + type.getSimpleName() + ")myObject";
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
            expression = '(' + type.getSimpleName() + ")myObject";
            if (type.isPrimitive() && type != boolean.class) {
                resultExpression = TypeUtils.getUnboxedType(type) + "Cast(myObject)";
            } else {
                resultExpression = expression;
            }
            check(expression, resultExpression, type, new String[] {"myObject"});
        }
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
        resultExpression = "greater(myInt, plus(1, divide(multiply(2, myInt), 4)))||myBoolean";
        check(expression, resultExpression, boolean.class, new String[] {"myBoolean", "myInt"});

        expression = "myInt>1+2*myInt/4 | myBoolean";
        resultExpression = "binaryOr(greater(myInt, plus(1, divide(multiply(2, myInt), 4))), myBoolean)";
        check(expression, resultExpression, Boolean.class, new String[] {"myBoolean", "myInt"});

        expression = "myInt+myString";
        resultExpression = "myInt+myString";
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
        resultExpression = "not(myBoolean)";
        check(expression, resultExpression, Boolean.class, new String[] {"myBoolean"});

        expression = "(String)myString==null";
        resultExpression = "eq((String)myString, null)";
        check(expression, resultExpression, boolean.class, new String[] {"myString"});

        expression = "1==1 ? myString : null";
        resultExpression = "eq(1, 1) ? myString : null";
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
        resultExpression = "eq(1, 1) ? (Boolean)true : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[0]);

        expression = "1==1 ? true && true : null";
        resultExpression = "eq(1, 1) ? (Boolean)(true&&true) : NULL_BOOLEAN";
        check(expression, resultExpression, Boolean.class, new String[0]);

        expression = "myDoubleArray.length";
        resultExpression = "myDoubleArray.length";
        check(expression, resultExpression, int.class, new String[] {"myDoubleArray"});
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

        expression = "myArrayList[15]";
        resultExpression = "myArrayList.get(15)";
        check(expression, resultExpression, Object.class, new String[] {"myArrayList"});

        expression = "myHashMap[\"test\"]";
        resultExpression = "myHashMap.get(\"test\")";
        check(expression, resultExpression, Object.class, new String[] {"myHashMap"});

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

        expression = "io.deephaven.db.tables.ColumnDefinition.COLUMNTYPE_NORMAL";
        resultExpression = "io.deephaven.db.tables.ColumnDefinition.COLUMNTYPE_NORMAL";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "ColumnDefinition.COLUMNTYPE_NORMAL";
        resultExpression = "ColumnDefinition.COLUMNTYPE_NORMAL";
        check(expression, resultExpression, int.class, new String[] {});

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

        expression = "DBLanguageParserDummyClass.functionWithInterfacesAsArgTypes(`test`, 0)";
        resultExpression = "DBLanguageParserDummyClass.functionWithInterfacesAsArgTypes(\"test\", 0)";
        check(expression, resultExpression, int.class, new String[] {});
    }


    // TODO: Enable this test & make it pass. (IDS-571)
    // public void testMethodOverloading() throws Exception {
    // String expression="DBLanguageParserDummyClass.overloadedStaticMethod()";
    // String resultExpression="DBLanguageParserDummyClass.overloadedStaticMethod()";
    // check(expression, resultExpression, int.class, new String[]{});
    //
    // expression="DBLanguageParserDummyClass.overloadedStaticMethod(`test`)";
    // resultExpression="DBLanguageParserDummyClass.overloadedStaticMethod(\"test\")";
    // check(expression, resultExpression, int.class, new String[]{});
    //
    // expression="DBLanguageParserDummyClass.overloadedStaticMethod(`test1`, `test2`)";
    // resultExpression="DBLanguageParserDummyClass.overloadedStaticMethod(\"test1\", \"test2\")";
    // check(expression, resultExpression, int.class, new String[]{});
    //
    // expression="myDummyClass.overloadedMethod()";
    // resultExpression="myDummyClass.overloadedMethod()";
    // check(expression, resultExpression, int.class, new String[]{"myDummyClass"});
    //
    // expression="myDummyClass.overloadedMethod(`test`)";
    // resultExpression="myDummyClass.overloadedMethod(\"test\")";
    // check(expression, resultExpression, int.class, new String[]{"myDummyClass"});
    //
    // expression="myDummyClass.overloadedMethod(`test1`, `test2`)";
    // resultExpression="myDummyClass.overloadedMethod(\"test1\", \"test2\")";
    // check(expression, resultExpression, int.class, new String[]{"myDummyClass"});
    //
    // expression="myClosure.call()";
    // resultExpression="myClosure.call()";
    // check(expression, resultExpression, Object.class, new String[]{"myClosure"});
    //
    // expression="myClosure.call(1)";
    // resultExpression="myClosure.call(1)";
    // check(expression, resultExpression, Object.class, new String[]{"myClosure"});
    //
    // expression="myClosure.call(1, 2, 3)";
    // resultExpression="myClosure.call(1, 2, 3)";
    // check(expression, resultExpression, Object.class, new String[]{"myClosure"});
    // }

    /**
     * Test implicit argument type conversions (e.g. primitive casts and converting DbArrays to Java arrays)
     */
    public void testImplicitConversion() throws Exception {
        String expression = "testImplicitConversion1(myInt, myDouble, myLong, myInt, myDouble, myLong)";
        String resultExpression =
                "testImplicitConversion1(new double[]{ doubleCast(myInt), myDouble, doubleCast(myLong), doubleCast(myInt), myDouble, doubleCast(myLong) })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDouble", "myInt", "myLong"});

        expression = "testVarArgs(myInt, 'a', myDouble, 1.0, 5.0, myDouble)";
        resultExpression = "testVarArgs(myInt, 'a', new double[]{ myDouble, 1.0, 5.0, myDouble })";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDouble", "myInt"});

        expression = "testVarArgs(myInt, 'a', myDoubleArray)";
        resultExpression = "testVarArgs(myInt, 'a', myDoubleArray)";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDoubleArray", "myInt"});

        expression = "testImplicitConversion1(myDoubleDBArray)";
        resultExpression = "testImplicitConversion1(ArrayUtils.nullSafeDbArrayToArray(myDoubleDBArray))";
        check(expression, resultExpression, new double[0].getClass(), new String[] {"myDoubleDBArray"});

        expression = "testImplicitConversion2(myInt, myInt)";
        resultExpression = "testImplicitConversion2(new int[]{ myInt, myInt })";
        check(expression, resultExpression, new int[0].getClass(), new String[] {"myInt"});

        expression = "testImplicitConversion3(myDBArray)";
        resultExpression = "testImplicitConversion3(ArrayUtils.nullSafeDbArrayToArray(myDBArray))";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDBArray"});

        // expression="testImplicitConversion3(myDoubleArray)"; // TODO: This test fails.
        // resultExpression="testImplicitConversion3(myDoubleArray)"; // we should *not* convert from DbDoubleArray to
        // Object[]!
        // check(expression, resultExpression, new Object[0].getClass(), new String[]{"myDoubleArray"});

        expression = "testImplicitConversion3((Object) myDoubleArray)";
        resultExpression = "testImplicitConversion3((Object)myDoubleArray)"; // test a workaround for the above
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleArray"});

        expression = "testImplicitConversion3(myDoubleArray, myInt)";
        resultExpression = "testImplicitConversion3(myDoubleArray, myInt)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDoubleArray", "myInt"});

        expression = "testImplicitConversion4(myInt, myDBArray)";
        resultExpression = "testImplicitConversion4(doubleCast(myInt), ArrayUtils.nullSafeDbArrayToArray(myDBArray))";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDBArray", "myInt"});

        expression = "testImplicitConversion4(myInt, myDBArray, myDouble)";
        resultExpression = "testImplicitConversion4(doubleCast(myInt), myDBArray, myDouble)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDBArray", "myDouble", "myInt"});

        expression = "testImplicitConversion4(myInt, myDouble, myDBArray)";
        resultExpression = "testImplicitConversion4(doubleCast(myInt), myDouble, myDBArray)";
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDBArray", "myDouble", "myInt"});

        // expression="testImplicitConversion5(myDBArray)"; // TODO: This test fails (declared arg type is
        // "DbArrayBase...")
        // resultExpression="testImplicitConversion5(myDBArray)"; // vararg of DbArrayBase -- don't convert!
        // check(expression, resultExpression, new Object[0].getClass(), new String[]{"myDBArray"});

        expression = "testImplicitConversion5((DbArrayBase) myDBArray)"; // Workaround for the above.
        resultExpression = "testImplicitConversion5((DbArrayBase)myDBArray)"; // vararg of DbArrayBase -- don't convert!
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDBArray"});

        expression = "testImplicitConversion5(myDBArray, myDBArray)";
        resultExpression = "testImplicitConversion5(myDBArray, myDBArray)"; // vararg of DbArrayBase -- don't convert!
        check(expression, resultExpression, new Object[0].getClass(), new String[] {"myDBArray"});
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
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }
    }

    public void testFieldAccess() throws Exception {

        String expression = "`a_b_c_d_e`.split(`_`).length";
        String resultExpression = "\"a_b_c_d_e\".split(\"_\").length";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "new DBLanguageParserDummyClass().value";
        resultExpression = "new DBLanguageParserDummyClass().value";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "DBLanguageParserDummyClass.StaticNestedClass.staticVar";
        resultExpression = "DBLanguageParserDummyClass.StaticNestedClass.staticVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "DBLanguageParserDummyClass.StaticNestedClass.staticInstanceOfStaticClass.instanceVar";
        resultExpression = "DBLanguageParserDummyClass.StaticNestedClass.staticInstanceOfStaticClass.instanceVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "new DBLanguageParserDummyClass.StaticNestedClass().instanceVar";
        resultExpression = "new DBLanguageParserDummyClass.StaticNestedClass().instanceVar";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "myDummyClass.InnerClass";
        resultExpression = "myDummyClass.InnerClass";
        check(expression, resultExpression, DBLanguageParserDummyClass.InnerClass.class, new String[] {"myDummyClass"});

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
        check(expression, resultExpression, DBLanguageParserDummyClass.InnerClass.InnerInnerClass.class,
                new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClassInstance.innerInnerClassInstance.innerInnerInstanceVar";
        resultExpression = "myDummyClass.innerClassInstance.innerInnerClassInstance.innerInnerInstanceVar";
        check(expression, resultExpression, String.class, new String[] {"myDummyClass"});

        expression = "myDummyClass.innerClass2Instance.innerClassAsInstanceOfAnotherInnerClass";
        resultExpression = "myDummyClass.innerClass2Instance.innerClassAsInstanceOfAnotherInnerClass";
        check(expression, resultExpression, DBLanguageParserDummyClass.InnerClass.class, new String[] {"myDummyClass"});

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
        p.setProperty("DBLanguageParser.verboseExceptionMessages", "false"); // Better to test with non-verbose messages
        try {
            // First, test just bad field name
            try {
                expression = "myDummyInnerClass.staticVarThatDoesNotExist";
                resultExpression = "myDummyInnerClass.staticVarThatDoesNotExist";
                check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});
                fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
            } catch (DBLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : myDummyInnerClass") ||
                        !ex.getMessage().contains("Field Name : staticVarThatDoesNotExist")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }

            // Then do the same thing on a class name (not a variable)
            try {
                expression = "DBLanguageParserDummyClass.StaticNestedClass.staticVarThatDoesNotExist";
                resultExpression = "DBLanguageParserDummyClass.StaticNestedClass.staticVarThatDoesNotExist";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
            } catch (DBLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : DBLanguageParserDummyClass.StaticNestedClass") ||
                        !ex.getMessage().contains("Field Name : staticVarThatDoesNotExist")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }

            // Next, test bad scope
            try {
                expression = "myDummyNonExistentInnerClass.staticVar";
                resultExpression = "myDummyNonExistentInnerClass.staticVar";
                check(expression, resultExpression, String.class, new String[] {"myDummyInnerClass"});
                fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
            } catch (DBLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : myDummyNonExistentInnerClass") ||
                        !ex.getMessage().contains("Field Name : staticVar")) {
                    fail("Useless exception message!\nOriginal exception:\n" + ExceptionUtils.getStackTrace(ex));
                }
            }


            try {
                expression = "DBLanguageParserNonExistentDummyClass.StaticNestedClass.staticVar";
                resultExpression = "DBLanguageParserNonExistentDummyClass.StaticNestedClass.staticVar";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
            } catch (DBLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : DBLanguageParserNonExistentDummyClass.StaticNestedClass") ||
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
                        "io.deephaven.db.tables.lang.DBLanguageParserDummyClass.interpolate(myDoubleDBArray.toArray(),myDoubleDBArray.toArray(),new double[]{myDouble},io.deephaven.db.tables.lang.NestedEnum.ONE,false)[0]";
                resultExpression =
                        "io.deephaven.db.tables.lang.DBLanguageParserDummyClass.interpolate(myDoubleDBArray.toArray(),myDoubleDBArray.toArray(),new double[]{myDouble},io.deephaven.db.tables.lang.NestedEnum.ONE,false)[0]";
                check(expression, resultExpression, String.class, new String[] {});
                fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
            } catch (DBLanguageParser.QueryLanguageParseException ex) {
                if (!ex.getMessage().contains("Scope      : io.deephaven.db.tables.lang.NestedEnum") ||
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
                fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
            } catch (DBLanguageParser.QueryLanguageParseException ex) {
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
        check(expression, resultExpression, DBLanguageParserDummyEnum.class, new String[] {"myEnumValue"});

        expression = "myEnumValue.getAttribute()";
        resultExpression = "myEnumValue.getAttribute()";
        check(expression, resultExpression, String.class, new String[] {"myEnumValue"});

        expression = "DBLanguageParserDummyEnum.ONE";
        resultExpression = "DBLanguageParserDummyEnum.ONE";
        check(expression, resultExpression, DBLanguageParserDummyEnum.class, new String[] {});

        expression = "DBLanguageParserDummyEnum.ONE.getAttribute()";
        resultExpression = "DBLanguageParserDummyEnum.ONE.getAttribute()";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "DBLanguageParserDummyInterface.AnEnum.THING_ONE";
        resultExpression = "DBLanguageParserDummyInterface.AnEnum.THING_ONE";
        check(expression, resultExpression, DBLanguageParserDummyInterface.AnEnum.class, new String[] {});

        expression = "io.deephaven.db.tables.lang.DBLanguageParserDummyInterface.AnEnum.THING_ONE";
        resultExpression = "io.deephaven.db.tables.lang.DBLanguageParserDummyInterface.AnEnum.THING_ONE";
        check(expression, resultExpression, DBLanguageParserDummyInterface.AnEnum.class, new String[] {});

        expression = "DBLanguageParserDummyClass.SubclassOfDBLanguageParserDummyClass.EnumInInterface.THING_ONE";
        resultExpression = "DBLanguageParserDummyClass.SubclassOfDBLanguageParserDummyClass.EnumInInterface.THING_ONE";
        check(expression, resultExpression,
                DBLanguageParserDummyClass.SubclassOfDBLanguageParserDummyClass.EnumInInterface.class, new String[] {});

        expression = "DBLanguageParserDummyClass.functionWithEnumAsArgs(DBLanguageParserDummyEnum.ONE)";
        resultExpression = "DBLanguageParserDummyClass.functionWithEnumAsArgs(DBLanguageParserDummyEnum.ONE)";
        check(expression, resultExpression, int.class, new String[] {});

        expression =
                "DBLanguageParserDummyClass.functionWithEnumAsArgs(DBLanguageParserDummyInterface.AnEnum.THING_ONE)";
        resultExpression =
                "DBLanguageParserDummyClass.functionWithEnumAsArgs(DBLanguageParserDummyInterface.AnEnum.THING_ONE)";
        check(expression, resultExpression, int.class, new String[] {});

        expression = "DBLanguageParserDummyClass.functionWithEnumVarArgs(myEnumValue, DBLanguageParserDummyEnum.ONE)";
        resultExpression =
                "DBLanguageParserDummyClass.functionWithEnumVarArgs(myEnumValue, DBLanguageParserDummyEnum.ONE)";
        check(expression, resultExpression, int.class, new String[] {"myEnumValue"});
    }

    public void testBoxing() throws Exception {
        String expression = "1+myIntObj";
        String resultExpression = "plus(1, myIntObj.intValue())";
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
        resultExpression = "greater(myInt, plus(1, divide(multiply(2, myIntObj.intValue()), 4)))||myBooleanObj";
        check(expression, resultExpression, boolean.class, new String[] {"myBooleanObj", "myInt", "myIntObj"});

        expression = "myIntObj+myString";
        resultExpression = "myIntObj+myString";
        check(expression, resultExpression, String.class, new String[] {"myIntObj", "myString"});

        expression = "myIntObj>2";
        resultExpression = "greater(myIntObj.intValue(), 2)";
        check(expression, resultExpression, boolean.class, new String[] {"myIntObj"});
    }

    public void testEqualsConversion() throws Exception {
        DBLanguageParser.Result result =
                new DBLanguageParser("1==1", null, null, staticImports, null, null).getResult();
        assertEquals("eq(1, 1)", result.getConvertedExpression());

        result = new DBLanguageParser("1=1", null, null, staticImports, null, null).getResult();
        assertEquals("eq(1, 1)", result.getConvertedExpression());

        result = new DBLanguageParser("`me`=`you`", null, null, staticImports, null, null).getResult();
        assertEquals("eq(\"me\", \"you\")", result.getConvertedExpression());

        result = new DBLanguageParser("1=1 || 2=2 && (3=3 && 4==4)", null, null, staticImports, null, null).getResult();
        assertEquals("eq(1, 1)||eq(2, 2)&&(eq(3, 3)&&eq(4, 4))", result.getConvertedExpression());

        result = new DBLanguageParser("1<=1", null, null, staticImports, null, null).getResult();
        assertEquals("lessEquals(1, 1)", result.getConvertedExpression());

        result = new DBLanguageParser("1>=1", null, null, staticImports, null, null).getResult();
        assertEquals("greaterEquals(1, 1)", result.getConvertedExpression());

        result = new DBLanguageParser("1!=1", null, null, staticImports, null, null).getResult();
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
        resultExpression = "new int[]{ 1, 2, 3, 4 }";
        check(expression, resultExpression, new int[0].getClass(), new String[] {});

        expression = "new int[] {1,2,3,4}";
        resultExpression = "new int[]{ 1, 2, 3, 4 }"; // note that the parser alters spacing
        check(expression, resultExpression, new int[0].getClass(), new String[] {});

        expression = "new String[]{ `This`, `is`, `a`, `test` }";
        resultExpression = "new String[]{ \"This\", \"is\", \"a\", \"test\" }";
        check(expression, resultExpression, new String[0].getClass(), new String[] {});

        expression = "new SubclassOfDBLanguageParserDummyClass[] { " +
                "DBLanguageParserDummyClass.innerClassInstance, " +
                "DBLanguageParserDummyClass.innerClass2Instance, " +
                "new DBLanguageParserDummyClass.StaticNestedClass(), " +
                "myDummyInnerClass }";
        resultExpression = "new SubclassOfDBLanguageParserDummyClass[]{ " +
                "DBLanguageParserDummyClass.innerClassInstance, " +
                "DBLanguageParserDummyClass.innerClass2Instance, " +
                "new DBLanguageParserDummyClass.StaticNestedClass(), " +
                "myDummyInnerClass }";
        check(expression, resultExpression,
                new DBLanguageParserDummyClass.SubclassOfDBLanguageParserDummyClass[0].getClass(),
                new String[] {"myDummyInnerClass"});
    }

    public void testArraysAsArguments() throws Exception {
        String expression = "DBLanguageParserDummyClass.arrayAndDbArrayFunction(myIntDBArray)";
        String resultExpression = "DBLanguageParserDummyClass.arrayAndDbArrayFunction(myIntDBArray)";
        check(expression, resultExpression, long.class, new String[] {"myIntDBArray"});

        expression = "DBLanguageParserDummyClass.arrayAndDbArrayFunction(myIntArray)";
        resultExpression = "DBLanguageParserDummyClass.arrayAndDbArrayFunction(myIntArray)";
        check(expression, resultExpression, long.class, new String[] {"myIntArray"});

        expression = "DBLanguageParserDummyClass.arrayOnlyFunction(myIntDBArray)";
        resultExpression =
                "DBLanguageParserDummyClass.arrayOnlyFunction(ArrayUtils.nullSafeDbArrayToArray(myIntDBArray))";
        check(expression, resultExpression, long.class, new String[] {"myIntDBArray"});

        expression = "DBLanguageParserDummyClass.dbArrayOnlyFunction(myIntArray)";
        resultExpression = "DBLanguageParserDummyClass.dbArrayOnlyFunction(myIntArray)";
        try {
            // We don't (currently?) support converting primitive arrays to DB arrays.
            check(expression, resultExpression, int.class, new String[] {"myIntArray"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ex) {
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
        resultExpression = "new String(new char[]{ 'a', 'b', 'c', 'd', 'e' }, 1, 4)";
        check(expression, resultExpression, String.class, new String[] {});

        expression = "new HashSet()";
        resultExpression = "new HashSet()";
        check(expression, resultExpression, HashSet.class, new String[] {});

        expression = "new HashSet<String>()";
        resultExpression = "new HashSet<String>()";
        check(expression, resultExpression, HashSet.class, new String[] {});

        expression = "new io.deephaven.db.tables.lang.DBLanguageParserDummyClass()";
        resultExpression = "new io.deephaven.db.tables.lang.DBLanguageParserDummyClass()";
        check(expression, resultExpression, DBLanguageParserDummyClass.class, new String[] {});

        expression = "new DBLanguageParserDummyClass()";
        resultExpression = "new DBLanguageParserDummyClass()";
        check(expression, resultExpression, DBLanguageParserDummyClass.class, new String[] {});

        expression = "new DBLanguageParserDummyClass(myInt)";
        resultExpression = "new DBLanguageParserDummyClass(myInt)";
        check(expression, resultExpression, DBLanguageParserDummyClass.class, new String[] {"myInt"});

        expression = "new DBLanguageParserDummyClass.StaticNestedClass()";
        resultExpression = "new DBLanguageParserDummyClass.StaticNestedClass()";
        check(expression, resultExpression, DBLanguageParserDummyClass.StaticNestedClass.class, new String[] {});

        expression = "new io.deephaven.db.tables.utils.DBDateTime(123L)";
        resultExpression = "new io.deephaven.db.tables.utils.DBDateTime(123L)";
        check(expression, resultExpression, DBDateTime.class, new String[] {});
    }

    public void testIntToLongConverstion() throws Exception {
        String expression = "1+1283209200466";
        String resultExpression = "plus(1, 1283209200466L)";
        check(expression, resultExpression, long.class, new String[] {});

        expression = "1 + -1283209200466";
        resultExpression = "plus(1, negate(1283209200466L))";
        check(expression, resultExpression, long.class, new String[] {});
    }

    public void testGenerics() throws Exception {
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

        expression = "genericGetKey(myHashMap)";
        resultExpression = "genericGetKey(myHashMap)";
        check(expression, resultExpression, Integer.class, new String[] {"myHashMap"});

        expression = "genericGetValue(myHashMap)";
        resultExpression = "genericGetValue(myHashMap)";
        check(expression, resultExpression, Double.class, new String[] {"myHashMap"});
    }

    public void testDBArrayUnboxing() throws Exception {
        String expression = "genericArrayToSingle(myDBArray)";
        String resultExpression = "genericArrayToSingle(ArrayUtils.nullSafeDbArrayToArray(myDBArray))";
        check(expression, resultExpression, Double.class, new String[] {"myDBArray"});

        expression = "genericArraysToSingle(myDBArray, myIntegerObjArray)";
        resultExpression = "genericArraysToSingle(ArrayUtils.nullSafeDbArrayToArray(myDBArray), myIntegerObjArray)";
        check(expression, resultExpression, Integer.class, new String[] {"myDBArray", "myIntegerObjArray"});

        expression = "genericArraysToSingle(myIntegerObjArray, myDBArray)";
        resultExpression = "genericArraysToSingle(myIntegerObjArray, ArrayUtils.nullSafeDbArrayToArray(myDBArray))";
        check(expression, resultExpression, Double.class, new String[] {"myDBArray", "myIntegerObjArray"});

        expression = "genericDBArray(myDBArray)";
        resultExpression = "genericDBArray(myDBArray)";
        check(expression, resultExpression, Double.class, new String[] {"myDBArray"});

        expression = "genericArrayToSingle(myObjectDBArray)";
        resultExpression = "genericArrayToSingle(ArrayUtils.nullSafeDbArrayToArray(myObjectDBArray))";
        check(expression, resultExpression, Object.class, new String[] {"myObjectDBArray"});

        expression = "intArrayToInt(myIntDBArray)";
        resultExpression = "intArrayToInt(ArrayUtils.nullSafeDbArrayToArray(myIntDBArray))";
        check(expression, resultExpression, int.class, new String[] {"myIntDBArray"});

        expression = "myIntDBArray==myIntDBArray";
        resultExpression =
                "eqArray(ArrayUtils.nullSafeDbArrayToArray(myIntDBArray), ArrayUtils.nullSafeDbArrayToArray(myIntDBArray))";
        check(expression, resultExpression, boolean[].class, new String[] {"myIntDBArray"});

        expression = "booleanArrayToBoolean(myBooleanDBArray)";
        resultExpression = "booleanArrayToBoolean(ArrayUtils.nullSafeDbArrayToArray(myBooleanDBArray))";
        check(expression, resultExpression, Boolean.class, new String[] {"myBooleanDBArray"});

        expression = "new String(myByteArray)";
        resultExpression = "new String(myByteArray)";
        check(expression, resultExpression, String.class, new String[] {"myByteArray"});

        expression = "new String(myByteDBArray)";
        resultExpression = "new String(ArrayUtils.nullSafeDbArrayToArray(myByteDBArray))";
        check(expression, resultExpression, String.class, new String[] {"myByteDBArray"});
    }

    public void testInnerClasses() throws Exception {
        DBLanguageParser.Result result =
                new DBLanguageParser("io.deephaven.db.tables.lang.TestDBLanguageParser.InnerEnum.YEAH!=null", null,
                        null, staticImports, null, null).getResult();
        assertEquals("!eq(io.deephaven.db.tables.lang.TestDBLanguageParser.InnerEnum.YEAH, null)",
                result.getConvertedExpression());
    }

    public void testComplexExpressions() throws Exception {
        String expression =
                "java.util.stream.Stream.of(new String[]{ `a`, `b`, `c`, myInt > 0 ? myString=Double.toString(myDouble) ? `1` : `2` : new DBLanguageParserDummyClass().toString() }).count()";
        String resultExpression =
                "java.util.stream.Stream.of(new String[]{ \"a\", \"b\", \"c\", greater(myInt, 0) ? eq(myString, Double.toString(myDouble)) ? \"1\" : \"2\" : new DBLanguageParserDummyClass().toString() }).count()";
        check(expression, resultExpression, long.class, new String[] {"myDouble", "myInt", "myString"});

        expression = "myDummyClass.innerClassInstance.staticVar == 1_000_000L" +
                "? new int[] { java.util.stream.Stream.of(new String[] { `a`, `b`, `c`, myInt > 0 ? myString=Double.toString(myDouble) ? `1` : `2` : new DBLanguageParserDummyClass().toString() }).count() }"
                +
                ": myIntArray";
        resultExpression = "eq(myDummyClass.innerClassInstance.staticVar, 1_000_000L)" +
                " ? new int[]{ java.util.stream.Stream.of(new String[]{ \"a\", \"b\", \"c\", greater(myInt, 0) ? eq(myString, Double.toString(myDouble)) ? \"1\" : \"2\" : new DBLanguageParserDummyClass().toString() }).count() }"
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
                "io.deephaven.db.tables.lang.DBLanguageParserDummyClass.interpolate(myDoubleDBArray.toArray(),myDoubleDBArray.toArray(),new double[]{myDouble},io.deephaven.db.tables.lang.DBLanguageParserDummyClass.NestedEnum.ONE,false)[0]";
        resultExpression =
                "io.deephaven.db.tables.lang.DBLanguageParserDummyClass.interpolate(myDoubleDBArray.toArray(), myDoubleDBArray.toArray(), new double[]{ myDouble }, io.deephaven.db.tables.lang.DBLanguageParserDummyClass.NestedEnum.ONE, false)[0]";
        check(expression, resultExpression, double.class, new String[] {"myDouble", "myDoubleDBArray"});

        // For good measure, same test as above w/ implicit array conversions:
        expression =
                "DBLanguageParserDummyClass.interpolate(myDoubleDBArray,myDoubleDBArray,new double[]{myDouble},DBLanguageParserDummyClass.NestedEnum.ONE,false)[0]";
        resultExpression =
                "DBLanguageParserDummyClass.interpolate(ArrayUtils.nullSafeDbArrayToArray(myDoubleDBArray), ArrayUtils.nullSafeDbArrayToArray(myDoubleDBArray), new double[]{ myDouble }, DBLanguageParserDummyClass.NestedEnum.ONE, false)[0]";
        check(expression, resultExpression, double.class, new String[] {"myDouble", "myDoubleDBArray"});
    }

    public void testUnsupportedOperators() throws Exception {
        String expression, resultExpression;

        try {
            expression = resultExpression = "myInt++";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt--";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "++myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "--myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt += myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt -= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt *= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt /= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt /= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt &= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt |= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
        }

        try {
            expression = resultExpression = "myInt ^= myInt";
            check(expression, resultExpression, int.class, new String[] {"myInt"});
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (DBLanguageParser.QueryLanguageParseException ignored) {
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

        expression = "DBLanguageParserDummyClass.class";
        resultExpression = "DBLanguageParserDummyClass.class";
        check(expression, resultExpression, DBLanguageParserDummyClass.class.getClass(), new String[] {});

        expression = "DBLanguageParserDummyClass.InnerClass.class";
        resultExpression = "DBLanguageParserDummyClass.InnerClass.class";
        check(expression, resultExpression, DBLanguageParserDummyClass.InnerClass.class.getClass(), new String[] {});

        expression = "DBLanguageParserDummyInterface.class";
        resultExpression = "DBLanguageParserDummyInterface.class";
        check(expression, resultExpression, DBLanguageParserDummyInterface.class.getClass(), new String[] {});
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

    @SuppressWarnings("SameParameterValue")
    private void expectFailure(String expression, Class<?> resultType) throws Exception {
        try {
            check(expression, expression, resultType, new String[0]);
            fail("Should have thrown a DBLanguageParser.QueryLanguageParseException");
        } catch (QueryLanguageParseException expected) {
        }
    }

    private void check(String expression, String resultExpression, Class<?> resultType, String[] resultVarsUsed)
            throws Exception {
        DBLanguageParser.Result result = new DBLanguageParser(expression, packageImports, classImports, staticImports,
                variables, variableParameterizedTypes).getResult();

        assertEquals(resultType, result.getType());
        assertEquals(resultExpression, result.getConvertedExpression());

        String[] variablesUsed = result.getVariablesUsed().toArray(new String[0]);
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

    public static double[] testImplicitConversion1(double... d) {
        return d;
    }

    public static int[] testImplicitConversion2(int... i) {
        return i;
    }

    public static Object[] testImplicitConversion3(Object... o) {
        return o;
    }

    public static Object[] testImplicitConversion4(double d, Object... o) {
        return o;
    }

    public static Object[] testImplicitConversion5(DbArrayBase... o) {
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

    public static <T> T genericDBArray(DbArray<T> dbArray) {
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

