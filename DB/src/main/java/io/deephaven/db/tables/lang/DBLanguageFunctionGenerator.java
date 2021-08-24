/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.lang;

import io.deephaven.util.type.TypeUtils;
import com.github.javaparser.ast.expr.BinaryExpr;

import java.io.*;
import java.text.*;
import java.time.LocalDate;

public class DBLanguageFunctionGenerator {

    public static double[] plus(double a[], int b) {
        double[] ret = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = a[i] + b;
        }

        return ret;
    }

    public static void main(String args[]) {
        final long start = System.currentTimeMillis();
        // 0 - operation name
        // 1 - type of param 1
        // 2 - type of param 2
        // 3 - promoted type
        // 4 - null of param 1
        // 5 - null of param 2
        // 6 - null of promoted type
        // 7 - operation symbol
        // 8 - optional cast (for non-fp division)
        // 9 - optional compareTo thing
        // 10 - optional operation description for exception message (only for arrayArrayFormatter)
        // 11 - optional nonzero literal value of param type 1 (for testing)
        // 12 - optional nonzero literal value of param type 2 (for testing)

        MessageFormat varVarFormatter = new MessageFormat("" +
            "    public static {3} {0}({1} a, {2} b)'{'\n" +
            "        return a==QueryConstants.NULL_{4} || b==QueryConstants.NULL_{5} ? QueryConstants.NULL_{6} : a{7}{8}b;\n"
            +
            "    '}'");

        MessageFormat varVarTestFormatter = new MessageFormat("" +
            "    public static void test_{0}_{1}_{2}() '{'\n" +
            "        final {1} value1 = {11};\n" +
            "        final {2} value2 = {12};\n" +
            "        final {1} zero1 = 0;\n" +
            "        final {2} zero2 = 0;\n" +
            "\n" +
            "        {3} dbResult = -1, expectedResult = -1;\n" +
            "        int compareResult;\n" +
            "        String description;\n" +
            "\n" +
            "        try '{'\n" +
            "            dbResult = DBLanguageFunctionUtil.{0}(value1, value2);\n" +
            "            expectedResult = value1{7}{8}value2;\n" +
            "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
            "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(value1, value2), value1{7}{8}value2)\";\n"
            +
            "            TestCase.assertEquals(description, 0, compareResult);\n" +
            /*
             * ---------- This one runs into ArithmeticExceptions doing stuff like 0 % 0 ----------
             * "\n" + "            dbResult = DBLanguageFunctionUtil.{0}(value1, zero2);\n" +
             * "            expectedResult = value1{7}{8}zero2;\n" +
             * "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
             * "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(value1, zero2), value1{7}{8}zero2)\";\n"
             * + "            TestCase.assertEquals(description, 0, compareResult);\n" +
             */
            "\n" +
            "            dbResult = DBLanguageFunctionUtil.{0}(value1, QueryConstants.NULL_{5});\n"
            +
            "            expectedResult = QueryConstants.NULL_{6};\n" +
            "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
            "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(value1, QueryConstants.NULL_{5}), QueryConstants.NULL_{6})\";\n"
            +
            "            TestCase.assertEquals(description, 0, compareResult);\n" +
            "\n" +
            "            dbResult = DBLanguageFunctionUtil.{0}(zero1, value2);\n" +
            "            expectedResult = zero1{7}{8}value2;\n" +
            "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
            "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(zero1, value2), zero1{7}{8}value2)\";\n"
            +
            "            TestCase.assertEquals(description, 0, compareResult);\n" +
            "\n" +
            "            dbResult = DBLanguageFunctionUtil.{0}(QueryConstants.NULL_{4}, value2);\n"
            +
            "            expectedResult = QueryConstants.NULL_{6};\n" +
            "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
            "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(QueryConstants.NULL_{4}, value2), QueryConstants.NULL_{6})\";\n"
            +
            "            TestCase.assertEquals(description, 0, compareResult);\n" +
            "\n" +
            "            dbResult = DBLanguageFunctionUtil.{0}(QueryConstants.NULL_{4}, QueryConstants.NULL_{5});\n"
            +
            "            expectedResult = QueryConstants.NULL_{6};\n" +
            "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
            "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(QueryConstants.NULL_{4}, QueryConstants.NULL_{5}), QueryConstants.NULL_{6})\";\n"
            +
            "            TestCase.assertEquals(description, 0, compareResult);\n" +
            /*----------  Same issue as above  ----------
            "\n" +
            "            dbResult = DBLanguageFunctionUtil.{0}(zero1, zero2);\n" +
            "            expectedResult = zero1{7}{8}zero2;\n" +
            "            compareResult = {13}.compare(dbResult, expectedResult);\n" +
            "            description = \"{13}.compare(DBLanguageFunctionUtil.{0}(zero1, zero2), zero1{7}{8}zero2)\";\n" +
            "            TestCase.assertEquals(description, 0, compareResult);\n" +*/
            "        '}' catch (Exception ex) '{'\n" +
            "            throw new RuntimeException(\"Comparison failure: dbResult=\" + dbResult + \", expectedResult=\" + expectedResult, ex);\n"
            +
            "        '}'\n" +
            "\n" +
            "    '}'");

        // requires that value2 > value1
        MessageFormat varVarCompareTestFormatter = new MessageFormat("" +
            "    public static void test_compare_{1}_{2}_compare() '{'\n" +
            "        final {1} value1 = {11};\n" +
            "        final {2} value2 = {12};\n" +
            "        final {1} zero1 = 0;\n" +
            "        final {2} zero2 = 0;\n\n" +
            "        TestCase.assertEquals(-1, DBLanguageFunctionUtil.compareTo(value1, Float.NaN));\n"
            +
            "        TestCase.assertEquals(1, DBLanguageFunctionUtil.compareTo(Float.NaN, value1));\n"
            +
            "        TestCase.assertEquals(-1, DBLanguageFunctionUtil.compareTo(value1, Double.NaN));\n"
            +
            "        TestCase.assertEquals(1, DBLanguageFunctionUtil.compareTo(Double.NaN, value1));\n"
            +
            "        TestCase.assertEquals( 0, DBLanguageFunctionUtil.compareTo(zero1, zero2));\n" +
            "        TestCase.assertEquals( 0, DBLanguageFunctionUtil.compareTo(zero2, zero1));\n" +
            "        TestCase.assertEquals( 0, DBLanguageFunctionUtil.compareTo(QueryConstants.NULL_{4}, QueryConstants.NULL_{5}));\n"
            +
            "        TestCase.assertEquals( 0, DBLanguageFunctionUtil.compareTo(value1, value1));\n"
            +
            "        TestCase.assertEquals( 0, DBLanguageFunctionUtil.compareTo(value2, value2));\n"
            +
            "        TestCase.assertEquals(-1, DBLanguageFunctionUtil.compareTo(value1, value2));\n"
            +
            "        TestCase.assertEquals( 1, DBLanguageFunctionUtil.compareTo(value2, value1));\n"
            +
            "        TestCase.assertEquals(-1, DBLanguageFunctionUtil.compareTo(-value1, value2));\n"
            +
            "        TestCase.assertEquals(-1, DBLanguageFunctionUtil.compareTo(-value2, value1));\n"
            +
            "        TestCase.assertEquals( 1, DBLanguageFunctionUtil.compareTo(-value1, -value2));\n"
            +
            "    '}'");

        /*
         * Special varVar formatter for boolean operations. If one expression in a ternary if is a
         * boxed type and the other is a primitive, Java's inclination is to unbox the one that's
         * boxed.
         *
         * Since the DB uses {@code Boolean} to store booleans while supporting {@code null}, we
         * must manually box the result of boolean operations if we wish to support nulls.
         *
         * See JLS Chapter 15 section 25 --
         * https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.25"
         *
         * Note that we do not provide null-safe handling for the "conditional-and" and
         * "conditional-or" operators ("&&" and "||"). To do so while maintaining their
         * short-circuit behavior would require different parser changes.
         */
        MessageFormat varVarBooleanFormatter = new MessageFormat("" +
            "    public static {3} {0}({1} a, {2} b)'{'\n" +
            "        return a==QueryConstants.NULL_{4} || b==QueryConstants.NULL_{5} ? QueryConstants.NULL_{6} : Boolean.valueOf(a{7}{8}b);\n"
            +
            "    '}'");


        MessageFormat varVarCompareToFormatter = new MessageFormat("" +
            "    public static int compareTo({1} a, {2} b) '{'\n" +
            "        if (a==QueryConstants.NULL_{4})'{'\n" +
            "            return (b==QueryConstants.NULL_{5}) ? 0 : -1;\n" +
            "        '}'\n" +
            "        \n" +
            "        if (b==QueryConstants.NULL_{5})'{'\n" +
            "            return 1;\n" +
            "        '}'\n" +
            "\n" +
            "        return a<b ? -1 : (a==b ? 0 : 1);\n" +
            "    '}'");

        MessageFormat varFloatCompareToFormatter = new MessageFormat("" +
            "    public static int compareTo({1} a, {2} b) '{'\n" +
            "        if (a==QueryConstants.NULL_{4})'{'\n" +
            "            return (b==QueryConstants.NULL_{5}) ? 0 : -1;\n" +
            "        '}'\n" +
            "        \n" +
            "        if (b==QueryConstants.NULL_{5})'{'\n" +
            "            return 1;\n" +
            "        '}'\n" +
            "\n" +
            "        return Float.compare(a, b);\n" +
            "    '}'");

        MessageFormat varDoubleCompareToFormatter = new MessageFormat("" +
            "    public static int compareTo({1} a, {2} b) '{'\n" +
            "        if (a==QueryConstants.NULL_{4})'{'\n" +
            "            return (b==QueryConstants.NULL_{5}) ? 0 : -1;\n" +
            "        '}'\n" +
            "        \n" +
            "        if (b==QueryConstants.NULL_{5})'{'\n" +
            "            return 1;\n" +
            "        '}'\n" +
            "\n" +
            "        return Double.compare(a, b);\n" +
            "    '}'");

        // should cover (long,double) and (long,float) args
        MessageFormat longDoubleCompareToFormatter = new MessageFormat("" +
            "    public static int compareTo({1} a, {2} b) '{'\n" +
            "        if (a==QueryConstants.NULL_{4})'{'\n" +
            "            return (b==QueryConstants.NULL_{5}) ? 0 : -1;\n" +
            "        '}'\n" +
            "        \n" +
            "        if (b==QueryConstants.NULL_{5})'{'\n" +
            "            return 1;\n" +
            "        '}'\n\n" +
            "        if(Double.isNaN(b)) '{'\n" +
            "            return -1;\n" +
            "        }\n" +
            "        if(b > Long.MAX_VALUE) '{'\n" +
            "            return -1;\n" +
            "        '}' else if(b < Long.MIN_VALUE) '{'\n" +
            "            return 1;\n" +
            "        '}' else '{'\n" +
            "            final long longValue = (long) b;\n" +
            "            if (longValue > a) '{'\n" +
            "                return -1;\n" +
            "            '}' else if (longValue == a) '{'\n" +
            "                if (b - longValue == 0d) '{'\n" +
            "                    return 0;\n" +
            "                '}' else if (b - longValue > 0d) '{'\n" +
            "                    return -1;\n" +
            "                '}'\n" +
            "            '}'\n" +
            "            return 1;\n" +
            "        '}'\n" +
            "    '}'");

        MessageFormat inverseCompareToFormatter = new MessageFormat("" +
            "    public static int compareTo({1} a, {2} b) '{'\n" +
            "        return -compareTo(b, a);\n" +
            "    '}'");

        MessageFormat varVarCompareToUserFormatter = new MessageFormat("" +
            "    public static boolean {0}({1} a, {2} b)'{'\n" +
            "        return compareTo(a,b){9};\n" +
            "    '}'");

        MessageFormat varVarEqualsFormatter = new MessageFormat("" +
            "    public static boolean eq({1} a, {2} b) '{'\n" +
            "        if (a==QueryConstants.NULL_{4})'{'\n" +
            "            return (b==QueryConstants.NULL_{5});\n" +
            "        '}'\n" +
            "        \n" +
            "        if (b==QueryConstants.NULL_{5})'{'\n" +
            "            return false;\n" +
            "        '}'\n" +
            "\n" +
            "        return a==b;\n" +
            "    '}'");

        MessageFormat arrayArrayFormatter = new MessageFormat("" +
            "    public static {3}[] {0}Array({1} a[], {2} b[])'{'\n" +
            "        if (a.length != b.length) throw new IllegalArgumentException(\"Attempt to {10} two arrays ({1}, {2}) of different length\" +\n"
            +
            "                \" (a.length=\" + a.length + \", b.length=\" + b.length + '')'');\n" +
            "        \n" +
            "        {3}[] ret = new {3}[a.length];\n" +
            "        for (int i = 0; i < a.length; i++) '{'\n" +
            "            ret[i] = {0}(a[i],b[i]);\n" +
            "        '}'\n" +
            "        \n" +
            "        return ret;\n" +
            "    '}'");

        MessageFormat arrayVarFormatter = new MessageFormat("" +
            "    public static {3}[] {0}Array({1} a[], {2} b)'{'\n" +
            "        {3}[] ret = new {3}[a.length];\n" +
            "        for (int i = 0; i < a.length; i++) '{'\n" +
            "            ret[i] = {0}(a[i],b);\n" +
            "        '}'\n" +
            "\n" +
            "        return ret;\n" +
            "    '}'");

        MessageFormat varArrayFormatter = new MessageFormat("" +
            "    public static {3}[] {0}Array({1} a, {2} b[])'{'\n" +
            "        {3}[] ret = new {3}[b.length];\n" +
            "        for (int i = 0; i < b.length; i++) '{'\n" +
            "            ret[i] = {0}(a,b[i]);\n" +
            "        '}'\n" +
            "\n" +
            "        return ret;\n" +
            "    '}'");

        MessageFormat castFormatter = new MessageFormat("" +
            "    public static {2} {2}Cast({1} a)'{'\n" +
            "        return a==QueryConstants.NULL_{4} ? QueryConstants.NULL_{5} : ({2})a;\n" +
            "    '}'");

        /*
         * Note that this will only work when unboxing -- e.g. doubleCast(a) when 'a' is a Double.
         * Casting from an Integer to a double requires: doubleCast(intCast(theInteger))
         * 
         * See the language specification, or comments in the parser, for more details.
         */
        MessageFormat castFromObjFormatter = new MessageFormat("" +
            "    public static {1} {1}Cast(Object a)'{'\n" +
            "        return a==null ? QueryConstants.NULL_{4} : ({1})a;\n" +
            "    '}'");

        MessageFormat negateFormatter = new MessageFormat("" +
            "    public static {3} negate({1} a)'{'\n" +
            "        return a==QueryConstants.NULL_{4} ? QueryConstants.NULL_{6} : -a;\n" +
            "    '}'");

        final int sbCapacity = (int) Math.pow(2, 20);
        StringBuilder buf = new StringBuilder(sbCapacity);
        StringBuilder testBuf = new StringBuilder(sbCapacity);

        buf.append("/*\n" +
            " * Copyright (c) 2016-").append(LocalDate.now().getYear())
            .append(" Deephaven Data Labs and Patent Pending\n" +
                " * GENERATED CODE - DO NOT MODIFY DIRECTLY\n" +
                " * This class generated by " + DBLanguageFunctionGenerator.class.getCanonicalName()
                + "\n" +
                " */\n" +
                "\n");

        buf.append("package io.deephaven.db.tables.lang;\n\n");

        buf.append("import io.deephaven.util.QueryConstants;\n\n");

        buf.append(
            "@SuppressWarnings({\"unused\", \"WeakerAccess\", \"SimplifiableIfStatement\"})\n");
        buf.append("public final class DBLanguageFunctionUtil {\n\n");

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        buf.append("" +
            "    public static boolean eq(Object obj1, Object obj2){\n" +
            "        //noinspection SimplifiableBooleanExpression\n" +
            "        return obj1==obj2 || (!(obj1==null ^ obj2==null) && obj1.equals(obj2));\n" +
            "    }\n" +
            "    \n" +
            "    @SuppressWarnings({\"unchecked\"})\n" +
            "    public static int compareTo(Comparable obj1, Comparable obj2) {\n" +
            "        if (obj1==null){\n" +
            "            return (obj2==null) ? 0 : -1;\n" +
            "        }\n" +
            "        \n" +
            "        if (obj2==null){\n" +
            "            return 1;\n" +
            "        }\n" +
            "\n" +
            "        return obj1.compareTo(obj2);\n" +
            "    }\n" +
            "\n" +
            "    public static Boolean not(Boolean a){\n" +
            "        return a==QueryConstants.NULL_BOOLEAN ? QueryConstants.NULL_BOOLEAN : Boolean.valueOf(!a);\n"
            +
            "    }\n\n");

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------
        /* Now start the test class: */

        testBuf.append("/*\n" +
            " * Copyright (c) 2016-").append(LocalDate.now().getYear())
            .append(" Deephaven Data Labs and Patent Pending\n" +
                " * GENERATED CODE - DO NOT MODIFY DIRECTLY\n" +
                " * This class generated by " + DBLanguageFunctionGenerator.class.getCanonicalName()
                + "\n" +
                " */\n" +
                "\n");

        testBuf.append("package io.deephaven.db.tables.lang;\n\n");

        testBuf.append("import io.deephaven.util.QueryConstants;\n\n");
        testBuf.append("import junit.framework.TestCase;\n\n");

        testBuf.append("@SuppressWarnings({\"unused\", \"WeakerAccess\", \"NumericOverflow\"})\n");
        testBuf.append("public final class TestDBLanguageFunctionUtil extends TestCase {\n\n");

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        Class classes[] = new Class[] {int.class, double.class, long.class, float.class, char.class,
                byte.class, short.class};

        BinaryExpr.Operator operators[] = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.plus,
                BinaryExpr.Operator.minus,
                BinaryExpr.Operator.times,
                BinaryExpr.Operator.divide,
                BinaryExpr.Operator.remainder,
        };

        // Verbs corresponding to each operator, used in exception messages: "Attempt to _____ two
        // arrays of different length"
        String[] operatorDescriptions =
            new String[] {"add", "subtract", "multiply", "divide", "calculate remainder of"};

        for (int i = 0; i < operators.length; i++) {
            BinaryExpr.Operator operator = operators[i];
            String opDescription = operatorDescriptions[i];
            for (Class classA : classes) {
                for (Class classB : classes) {
                    append(buf, varVarFormatter, operator, classA, classB);
                    appendTest(testBuf, varVarTestFormatter, operator, classA, classB);
                    append(buf, arrayArrayFormatter, operator, opDescription, classA, classB);
                    append(buf, arrayVarFormatter, operator, classA, classB);
                    append(buf, varArrayFormatter, operator, classA, classB);
                }
            }
        }

        // compare tests
        for (Class classA : classes) {
            for (Class classB : classes) {
                appendTest(testBuf, varVarCompareTestFormatter, classA, classB,
                    getSmallLiteral(classA), getBiggerLiteral(classB));
            }
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, long.class, char.class, byte.class, short.class};

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.binOr,
                BinaryExpr.Operator.xor,
                BinaryExpr.Operator.binAnd
        };

        for (BinaryExpr.Operator operator : operators) {
            for (Class clazz : classes) {
                append(buf, varVarFormatter, operator, clazz, clazz);
                append(buf, arrayArrayFormatter, operator, operator.name(), clazz, clazz);
                append(buf, arrayVarFormatter, operator, clazz, clazz);
                append(buf, varArrayFormatter, operator, clazz, clazz);
            }
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, double.class, long.class, float.class, char.class,
                byte.class, short.class};

        for (Class classA : classes) {
            for (Class classB : classes) {

                // handle special cases with float/double arguments (need to handle NaN/precision
                // differently)
                if (classA.equals(long.class) && classB.equals(double.class)) {
                    append(buf, longDoubleCompareToFormatter, BinaryExpr.Operator.plus, classA,
                        classB); // the plus is just to avoid a npe
                } else if (classA.equals(double.class) && classB.equals(long.class)) {
                    append(buf, inverseCompareToFormatter, BinaryExpr.Operator.plus, classA,
                        classB); // the plus is just to avoid a npe
                } else if (classA.equals(long.class) && classB.equals(float.class)) {
                    append(buf, longDoubleCompareToFormatter, BinaryExpr.Operator.plus, classA,
                        classB); // the plus is just to avoid a npe
                } else if (classA.equals(float.class) && classB.equals(long.class)) {
                    append(buf, inverseCompareToFormatter, BinaryExpr.Operator.plus, classA,
                        classB); // the plus is just to avoid a npe
                } else if (classA.equals(double.class) || classB.equals(double.class)) {
                    // if either arg is a double, we promote to double
                    append(buf, varDoubleCompareToFormatter, BinaryExpr.Operator.plus, classA,
                        classB); // the plus is just to avoid a npe
                } else if (classA.equals(float.class) || classB.equals(float.class)) {
                    // if both args can contain a float, use float comparator, otherwise promote to
                    // double
                    if (classA.isAssignableFrom(float.class)
                        && classB.isAssignableFrom(float.class)) {
                        append(buf, varFloatCompareToFormatter, BinaryExpr.Operator.plus, classA,
                            classB); // the plus is just to avoid a npe
                    } else {
                        append(buf, varDoubleCompareToFormatter, BinaryExpr.Operator.plus, classA,
                            classB); // the plus is just to avoid a npe
                    }
                } else {
                    append(buf, varVarCompareToFormatter, BinaryExpr.Operator.plus, classA, classB); // the
                                                                                                     // plus
                                                                                                     // is
                                                                                                     // just
                                                                                                     // to
                                                                                                     // avoid
                                                                                                     // a
                                                                                                     // npe
                }

                append(buf, varVarEqualsFormatter, BinaryExpr.Operator.plus, classA, classB); // the
                                                                                              // plus
                                                                                              // is
                                                                                              // just
                                                                                              // to
                                                                                              // avoid
                                                                                              // a
                                                                                              // npe
                append(buf, arrayArrayFormatter, BinaryExpr.Operator.equals, "check equality of",
                    classA, classB);
                append(buf, arrayVarFormatter, BinaryExpr.Operator.equals, classA, classB);
                append(buf, varArrayFormatter, BinaryExpr.Operator.equals, classA, classB);
            }
        }

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.less,
                BinaryExpr.Operator.greater,
                BinaryExpr.Operator.lessEquals,
                BinaryExpr.Operator.greaterEquals
        };

        for (BinaryExpr.Operator operator : operators) {
            for (Class classA : classes) {
                for (Class classB : classes) {
                    append(buf, varVarCompareToUserFormatter, operator, classA, classB);
                    append(buf, arrayArrayFormatter, operator, "compare", classA, classB);
                    append(buf, arrayVarFormatter, operator, classA, classB);
                    append(buf, varArrayFormatter, operator, classA, classB);
                }
            }
        }

        for (BinaryExpr.Operator operator : operators) {
            append(buf, varVarCompareToUserFormatter, operator, Comparable.class, Comparable.class);
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.binOr, // no or and and because we like shortcircuit
                BinaryExpr.Operator.xor,
                BinaryExpr.Operator.binAnd,
        };

        for (BinaryExpr.Operator operator : operators) {
            append(buf, varVarBooleanFormatter, operator, Boolean.class, Boolean.class);
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        append(buf, arrayArrayFormatter, BinaryExpr.Operator.equals, "check equality of",
            Boolean.class, boolean.class);
        append(buf, arrayArrayFormatter, BinaryExpr.Operator.equals, "check equality of",
            boolean.class, Boolean.class);
        append(buf, arrayArrayFormatter, BinaryExpr.Operator.equals, "check equality of",
            boolean.class, boolean.class);
        append(buf, arrayArrayFormatter, BinaryExpr.Operator.equals, "check equality of",
            Object.class, Object.class);

        append(buf, arrayVarFormatter, BinaryExpr.Operator.equals, boolean.class, Boolean.class);
        append(buf, arrayVarFormatter, BinaryExpr.Operator.equals, Object.class, Object.class);

        append(buf, varArrayFormatter, BinaryExpr.Operator.equals, Boolean.class, boolean.class);
        append(buf, varArrayFormatter, BinaryExpr.Operator.equals, Object.class, Object.class);

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.less,
                BinaryExpr.Operator.greater,
                BinaryExpr.Operator.lessEquals,
                BinaryExpr.Operator.greaterEquals
        };

        for (BinaryExpr.Operator operator : operators) {
            append(buf, arrayArrayFormatter, operator, "compare", Comparable.class,
                Comparable.class);
            append(buf, arrayVarFormatter, operator, Comparable.class, Comparable.class);
            append(buf, varArrayFormatter, operator, Comparable.class, Comparable.class);
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, double.class, long.class, float.class, char.class,
                byte.class, short.class};

        for (Class classA : classes) { // Functions for null-safe casts between primitive types
            for (Class classB : classes) {
                if (classA != classB) { // don't create functions for redundant casts (e.g.
                                        // intCast(int))
                    append(buf, castFormatter, BinaryExpr.Operator.plus, classA, classB); // the
                                                                                          // plus is
                                                                                          // just so
                                                                                          // we
                                                                                          // don't
                                                                                          // get a
                                                                                          // npe
                }
            }
        }

        for (Class c : classes) { // Functions for null-safe casts from Object to primitive types
            append(buf, castFromObjFormatter, BinaryExpr.Operator.plus, c, Object.class); // the
                                                                                          // plus
                                                                                          // and
                                                                                          // Object
                                                                                          // are
                                                                                          // just so
                                                                                          // we
                                                                                          // don't
                                                                                          // get a
                                                                                          // npe
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, double.class, long.class, float.class, char.class,
                byte.class, short.class};

        for (Class clazz : classes) {
            append(buf, negateFormatter, BinaryExpr.Operator.plus, clazz, clazz); // the plus is
                                                                                  // just so we
                                                                                  // don't get a npe
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        buf.append("}\n");
        testBuf.append("}\n");

        String fileName =
            "./DB/src/main/java/io/deephaven/db/tables/lang/DBLanguageFunctionUtil.java";
        String testFileName =
            "./DB/src/test/java/io/deephaven/db/tables/lang/TestDBLanguageFunctionUtil.java";
        try {
            try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName))) {
                out.write(buf.toString());
            }

            try (BufferedWriter testOut = new BufferedWriter(new FileWriter(testFileName))) {
                testOut.write(testBuf.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Finished generating DBLanguageFunctionUtil in "
            + new DecimalFormat().format(System.currentTimeMillis() - start) + " millis");
        System.out.println("Wrote DBLanguageFunctionUtil to: " + fileName);
        System.out.println("Wrote TestDBLanguageFunctionUtil to: " + testFileName);
    }

    private static void append(StringBuilder buf, MessageFormat messageFormat,
        BinaryExpr.Operator op, Class type1, Class type2) {
        append(buf, messageFormat, op, null, type1, type2);
    }

    private static void append(StringBuilder buf, MessageFormat messageFormat,
        BinaryExpr.Operator op, String opDescription, Class type1, Class type2) {
        append(buf, messageFormat, op, opDescription, type1, type2, null, null);
    }

    private static void appendTest(StringBuilder buf, MessageFormat messageFormat,
        BinaryExpr.Operator op, Class type1, Class type2) {
        append(buf, messageFormat, op, null, type1, type2, getLiteral(type1), getLiteral(type2));
    }

    private static void appendTest(StringBuilder buf, MessageFormat messageFormat, Class type1,
        Class type2, String literal1, String literal2) {
        append(buf, messageFormat, null, null, type1, type2, literal1, literal2);
    }

    private static void append(StringBuilder buf, MessageFormat messageFormat,
        BinaryExpr.Operator op, String opDescription, Class type1, Class type2, String literal1,
        String literal2) {
        Class promotedType;

        if (op == BinaryExpr.Operator.equals || op == BinaryExpr.Operator.less
            || op == BinaryExpr.Operator.greater || op == BinaryExpr.Operator.lessEquals
            || op == BinaryExpr.Operator.greaterEquals) {
            promotedType = boolean.class;
        } else if (io.deephaven.util.type.TypeUtils.getBoxedType(type1) == Boolean.class
            || io.deephaven.util.type.TypeUtils.getBoxedType(type2) == Boolean.class) {
            promotedType = Boolean.class;
        } else {
            promotedType = DBLanguageParser.binaryNumericPromotionType(type1, type2);
        }

        String cast = "";

        if (op == BinaryExpr.Operator.divide && DBLanguageParser.isNonFPNumber(type2)) {
            cast = "(double)";
            promotedType = double.class;
        }

        String compareTo = "";

        if (op == BinaryExpr.Operator.less) {
            compareTo = "<0";
        } else if (op == BinaryExpr.Operator.greater) {
            compareTo = ">0";
        } else if (op == BinaryExpr.Operator.lessEquals) {
            compareTo = "<=0";
        } else if (op == BinaryExpr.Operator.greaterEquals) {
            compareTo = ">=0";
        }

        Class type1Unboxed = io.deephaven.util.type.TypeUtils.getUnboxedType(type1);
        Class type2Unboxed = io.deephaven.util.type.TypeUtils.getUnboxedType(type2);
        Class promotedTypeUnboxed = io.deephaven.util.type.TypeUtils.getUnboxedType(promotedType);

        final String operatorName = op == null ? null : DBLanguageParser.getOperatorName(op);
        final String operatorSymbol = op == null ? null : DBLanguageParser.getOperatorSymbol(op);
        buf.append(messageFormat.format(new Object[] {
                operatorName,
                type1.getSimpleName(),
                type2.getSimpleName(),
                promotedType.getSimpleName(),
                type1Unboxed == null ? "" : type1Unboxed.getSimpleName().toUpperCase(),
                type2Unboxed == null ? "" : type2Unboxed.getSimpleName().toUpperCase(),
                promotedTypeUnboxed == null ? ""
                    : promotedTypeUnboxed.getSimpleName().toUpperCase(),
                operatorSymbol,
                cast,
                compareTo,
                opDescription,
                literal1,
                literal2,
                TypeUtils.getBoxedType(promotedType).getSimpleName()
        })).append("\n\n");
    }


    /**
     * Returns a String of an example literal value of {@code type}. Used for generating tests.
     */
    private static String getLiteral(Class type) {
        if (type.equals(boolean.class)) {
            return "false";
        } else if (type.equals(char.class)) {
            return "'0'";
        } else if (type.equals(byte.class)) {
            return "(byte)42";
        } else if (type.equals(short.class)) {
            return "(short)42";
        } else if (type.equals(int.class)) {
            return "42";
        } else if (type.equals(long.class)) {
            return "42L";
        } else if (type.equals(float.class)) {
            return "42f";
        } else if (type.equals(double.class)) {
            return "42d";
        } else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    /**
     * Returns a String of an small example literal value of {@code type}. Used for generating
     * comparison tests.
     */
    private static String getSmallLiteral(Class type) {
        if (type.equals(boolean.class)) {
            return "false";
        } else if (type.equals(char.class)) {
            return "(char)1";
        } else if (type.equals(byte.class)) {
            return "(byte)1";
        } else if (type.equals(short.class)) {
            return "(short)1";
        } else if (type.equals(int.class)) {
            return "1";
        } else if (type.equals(long.class)) {
            return "1L";
        } else if (type.equals(float.class)) {
            return "0.01f";
        } else if (type.equals(double.class)) {
            return "0.01d";
        } else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    /**
     * Returns a String of an bigger example literal value of {@code type}. Must be larger than the
     * value produced by getSmallLiteral (across all types). Used for generating comparison tests.
     */
    private static String getBiggerLiteral(Class type) {
        if (type.equals(boolean.class)) {
            return "true";
        } else if (type.equals(char.class)) {
            return "'1'";
        } else if (type.equals(byte.class)) {
            return "(byte)42";
        } else if (type.equals(short.class)) {
            return "(short)42";
        } else if (type.equals(int.class)) {
            return "42";
        } else if (type.equals(long.class)) {
            return "42L";
        } else if (type.equals(float.class)) {
            return "42.0f";
        } else if (type.equals(double.class)) {
            return "42.0d";
        } else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }
}

