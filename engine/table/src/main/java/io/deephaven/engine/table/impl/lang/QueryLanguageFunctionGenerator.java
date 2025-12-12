//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

import com.github.javaparser.ast.expr.BinaryExpr;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.MessageFormat;

public class QueryLanguageFunctionGenerator {
    public static void main(String[] args) throws IOException {
        final long start = System.currentTimeMillis();

        final int sbCapacity = (int) Math.pow(2, 20);
        StringBuilder buf = new StringBuilder(sbCapacity);

        buf.append(String.join("\n",
                "//",
                "// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending",
                "//",
                "// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY",
                "// ****** Run " + QueryLanguageFunctionGenerator.class.getSimpleName() + " to regenerate",
                "//",
                "// @formatter:off",
                ""));

        buf.append("package io.deephaven.engine.table.impl.lang;\n\n");

        buf.append("import io.deephaven.configuration.Configuration;\n");
        buf.append("import io.deephaven.util.QueryConstants;\n");
        buf.append("import io.deephaven.util.annotations.UserInvocationPermitted;\n");
        buf.append("import org.jpy.PyObject;\n\n");

        buf.append("import java.math.BigDecimal;\n");
        buf.append("import java.math.BigInteger;\n");
        buf.append("import java.math.RoundingMode;\n\n");
        buf.append("import static java.lang.Math.*;\n\n");

        buf.append("@SuppressWarnings({\"unused\", \"WeakerAccess\", \"SimplifiableIfStatement\"})\n");
        buf.append("@UserInvocationPermitted(value = \"function_library\")\n");
        buf.append("public final class QueryLanguageFunctionUtils {\n\n");

        buf.append("" +
                "    private static final String DEFAULT_SCALE_PROPERTY = \"defaultScale\";\n" +
                "    public static final int DEFAULT_SCALE = Configuration.getInstance()\n" +
                "            .getIntegerForClassWithDefault(QueryLanguageFunctionUtils.class, DEFAULT_SCALE_PROPERTY, 8);\n\n"
                +
                "    public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;\n\n");

        buf.append("" +
                "    public static boolean eq(Object obj1, Object obj2) {\n" +
                "        // noinspection SimplifiableBooleanExpression\n" +
                "        return obj1 == obj2 || (!(obj1 == null ^ obj2 == null) && obj1.equals(obj2));\n" +
                "    }\n" +
                "    \n" +
                "    @SuppressWarnings({\"unchecked\"})\n" +
                "    public static int compareTo(Comparable obj1, Comparable obj2) {\n" +
                "        if (obj1 == null) {\n" +
                "            return (obj2 == null) ? 0 : -1;\n" +
                "        }\n" +
                "        \n" +
                "        if (obj2 == null) {\n" +
                "            return 1;\n" +
                "        }\n" +
                "\n" +
                "        return obj1.compareTo(obj2);\n" +
                "    }\n" +
                "\n" +
                "    public static boolean not(boolean a) {\n" +
                "        return !a;\n" +
                "    }\n" +
                "\n" +
                "    public static Boolean not(Boolean a) {\n" +
                "        return a == QueryConstants.NULL_BOOLEAN ? QueryConstants.NULL_BOOLEAN : Boolean.valueOf(!a);\n"
                +
                "    }\n\n");

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        Class<?>[] classes = new Class[] {int.class, double.class, long.class, float.class, char.class, byte.class,
                short.class, BigDecimal.class, BigInteger.class};

        BinaryExpr.Operator[] operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.PLUS,
                BinaryExpr.Operator.MINUS,
                BinaryExpr.Operator.MULTIPLY,
                BinaryExpr.Operator.DIVIDE,
                BinaryExpr.Operator.REMAINDER,
        };

        // Verbs corresponding to each operator, used in exception messages: "Attempt to _____ two arrays of different
        // length"
        String[] operatorDescriptions =
                new String[] {"add", "subtract", "multiply", "divide", "calculate remainder of"};

        for (int i = 0; i < operators.length; i++) {
            final BinaryExpr.Operator operator = operators[i];
            final String opDescription = operatorDescriptions[i];
            for (final Class<?> classA : classes) {
                for (final Class<?> classB : classes) {
                    buf.append(generateArithmeticFunction(operator, classA, classB));
                    buf.append(generateArrayArrayFunction(operator, classA, classB, opDescription));
                    buf.append(generateArrayVarFunction(operator, classA, classB));
                    buf.append(generateVarArrayFunction(operator, classA, classB));
                }
            }
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, long.class, char.class, byte.class, short.class};

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.BINARY_OR,
                BinaryExpr.Operator.XOR,
                BinaryExpr.Operator.BINARY_AND
        };
        operatorDescriptions =
                new String[] {"binary or", "binary xor", "binary and"};

        for (int i = 0; i < operators.length; i++) {
            final BinaryExpr.Operator operator = operators[i];
            final String opDescription = operatorDescriptions[i];
            for (final Class<?> clazz : classes) {
                buf.append(generateArithmeticFunction(operator, clazz, clazz));
                buf.append(generateArrayArrayFunction(operator, clazz, clazz, opDescription));
                buf.append(generateArrayVarFunction(operator, clazz, clazz));
                buf.append(generateVarArrayFunction(operator, clazz, clazz));
            }
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, double.class, long.class, float.class, char.class, byte.class, short.class,
                BigDecimal.class, BigInteger.class};

        for (final Class<?> classA : classes) {
            for (final Class<?> classB : classes) {
                buf.append(generateCompareToFunction(classA, classB));
                buf.append(generateEqualityFunction(classA, classB));
                buf.append(generateArrayArrayFunction(BinaryExpr.Operator.EQUALS, classA, classB, "check equality of"));
                buf.append(generateArrayVarFunction(BinaryExpr.Operator.EQUALS, classA, classB));
                buf.append(generateVarArrayFunction(BinaryExpr.Operator.EQUALS, classA, classB));
            }
        }

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.LESS,
                BinaryExpr.Operator.GREATER,
                BinaryExpr.Operator.LESS_EQUALS,
                BinaryExpr.Operator.GREATER_EQUALS
        };

        for (BinaryExpr.Operator operator : operators) {
            for (Class<?> classA : classes) {
                for (Class<?> classB : classes) {
                    buf.append(generateInequalityFunction(operator, classA, classB));
                    buf.append(generateArrayArrayFunction(operator, classA, classB, "compare"));
                    buf.append(generateArrayVarFunction(operator, classA, classB));
                    buf.append(generateVarArrayFunction(operator, classA, classB));
                }
            }
        }

        for (BinaryExpr.Operator operator : operators) {
            buf.append(generateInequalityFunction(operator, Comparable.class, Comparable.class));
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        // no or and and because we like shortcircuit
        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.BINARY_OR,
                BinaryExpr.Operator.XOR,
                BinaryExpr.Operator.BINARY_AND,
        };

        for (BinaryExpr.Operator operator : operators) {
            buf.append(generateArithmeticFunction(operator, Boolean.class, Boolean.class));
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        buf.append(generateArrayArrayFunction(BinaryExpr.Operator.EQUALS, Boolean.class, boolean.class,
                "check equality of"));
        buf.append(generateArrayArrayFunction(BinaryExpr.Operator.EQUALS, boolean.class, Boolean.class,
                "check equality of"));
        buf.append(generateArrayArrayFunction(BinaryExpr.Operator.EQUALS, boolean.class, boolean.class,
                "check equality of"));
        buf.append(generateArrayArrayFunction(BinaryExpr.Operator.EQUALS, Object.class, Object.class,
                "check equality of"));

        buf.append(generateArrayVarFunction(BinaryExpr.Operator.EQUALS, boolean.class, Boolean.class));
        buf.append(generateArrayVarFunction(BinaryExpr.Operator.EQUALS, Object.class, Object.class));

        buf.append(generateVarArrayFunction(BinaryExpr.Operator.EQUALS, Boolean.class, boolean.class));
        buf.append(generateVarArrayFunction(BinaryExpr.Operator.EQUALS, Object.class, Object.class));

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        operators = new BinaryExpr.Operator[] {
                BinaryExpr.Operator.LESS,
                BinaryExpr.Operator.GREATER,
                BinaryExpr.Operator.LESS_EQUALS,
                BinaryExpr.Operator.GREATER_EQUALS
        };

        for (BinaryExpr.Operator operator : operators) {
            buf.append(generateArrayArrayFunction(operator, Comparable.class, Comparable.class, "compare"));
            buf.append(generateArrayVarFunction(operator, Comparable.class, Comparable.class));
            buf.append(generateVarArrayFunction(operator, Comparable.class, Comparable.class));
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, double.class, long.class, float.class, char.class, byte.class, short.class};

        // Functions for null-safe casts between primitive types
        for (Class<?> classA : classes) {
            for (Class<?> classB : classes) {
                // don't create functions for redundant casts (e.g. intCast(int))
                if (classA != classB) {
                    buf.append(generateCastFunction(classA, classB));
                }
            }
        }

        // Functions for null-safe casts from Object to primitive types
        for (Class<?> c : classes) {
            buf.append(generateCastFromObjFunction(c));
        }

        // Special casts for PyObject to primitive
        buf.append("        public static int intPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_INT;\n");
        buf.append("            }\n");
        buf.append("            return o.getIntValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static double doublePyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_DOUBLE;\n");
        buf.append("            }\n");
        buf.append("            return o.getDoubleValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static long longPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_LONG;\n");
        buf.append("            }\n");
        buf.append("            return o.getLongValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static float floatPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_FLOAT;\n");
        buf.append("            }\n");
        buf.append("            return (float) o.getDoubleValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static char charPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_CHAR;\n");
        buf.append("            }\n");
        buf.append("            return (char) o.getIntValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static byte bytePyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_BYTE;\n");
        buf.append("            }\n");
        buf.append("            return (byte) o.getIntValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static short shortPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return QueryConstants.NULL_SHORT;\n");
        buf.append("            }\n");
        buf.append("            return (short) o.getIntValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static String doStringPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return null;\n");
        buf.append("            }\n");
        buf.append("            return o.getStringValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static boolean booleanPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                throw new NullPointerException(\"Provided value is unexpectedly null;");
        buf.append(" cannot cast to boolean\");\n");
        buf.append("            }\n");
        buf.append("            return o.getBooleanValue();\n");
        buf.append("        }\n\n");

        buf.append("        public static Boolean doBooleanPyCast(Object a) {\n");
        buf.append("            if (a != null && !(a instanceof PyObject)) {\n");
        buf.append("                throw new IllegalArgumentException(\"Provided value is not a PyObject\");\n");
        buf.append("            }\n");
        buf.append("            PyObject o = (PyObject) a;\n");
        buf.append("            if (o == null || o.isNone()) {\n");
        buf.append("                return null;\n");
        buf.append("            }\n");
        buf.append("            return o.getBooleanValue();\n");
        buf.append("        }\n\n");

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        classes = new Class[] {int.class, double.class, long.class, float.class, char.class, byte.class, short.class,
                BigDecimal.class, BigInteger.class};

        for (Class<?> clazz : classes) {
            buf.append(generateNegateFunction(clazz));
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        buf.append("}\n");

        String fileName =
                "./engine/table/src/main/java/io/deephaven/engine/table/impl/lang/QueryLanguageFunctionUtils.java";
        try {
            try (BufferedWriter out = new BufferedWriter(new FileWriter(fileName))) {
                out.write(buf.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Finished generating QueryLanguageFunctionUtils in "
                + new DecimalFormat().format(System.currentTimeMillis() - start) + " millis");
        System.out.println("Wrote QueryLanguageFunctionUtils to: " + fileName);
    }

    // region Generate Functions

    /**
     * Generate an arithmetic function for the given operator and argument types (e.g. plus/minus/multiply/divide).
     */
    private static String generateArithmeticFunction(
            @NotNull final BinaryExpr.Operator op,
            @NotNull final Class<?> classA,
            @NotNull final Class<?> classB) {
        final Class<?> returnType = getReturnType(op, classA, classB);

        final StringBuilder sb = new StringBuilder();

        final String operatorName = QueryLanguageParser.getOperatorName(op);
        sb.append(MessageFormat.format("    public static {0} {1}({2} a, {3} b) '{'\n",
                returnType.getSimpleName(),
                operatorName,
                classA.getSimpleName(),
                classB.getSimpleName()));

        final String nullA = nullForType(classA);
        final String nullB = nullForType(classB);
        final String nullReturn = nullForType(returnType);

        sb.append(MessageFormat.format("" +
                "        return a == {0} || b == {1} ? {2} : {3};\n",
                nullA,
                nullB,
                nullReturn,
                operatorStringForTypes(classA, classB, returnType, op)));

        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate a compareTo() function for the given argument types.
     */
    private static String generateCompareToFunction(@NotNull final Class<?> classA, @NotNull final Class<?> classB) {
        final StringBuilder sb = new StringBuilder();

        sb.append(MessageFormat.format("    public static int compareTo({0} a, {1} b) '{'\n",
                classA.getSimpleName(),
                classB.getSimpleName()));

        if (isFPNumber(classA) && classB == long.class) {
            // re-use the existing compareTo()
            sb.append("        return -compareTo(b, a);\n");
            sb.append("    }\n\n");
            return sb.toString();
        }

        if (isBigNumber(classA) || isBigNumber(classB)) {
            // NaN checks when converting to BigDecimal/BigInteger
            if (isFPNumber(classA)) {
                sb.append(MessageFormat.format("" +
                        "        if ({0}.isNaN(a)) '{'\n" +
                        "            return 1; // even if b == null\n" +
                        "        '}'\n",
                        TypeUtils.getBoxedType(classA).getSimpleName()));
            }
            if (isFPNumber(classB)) {
                sb.append(MessageFormat.format("" +
                        "        if ({0}.isNaN(b)) '{'\n" +
                        "            return -1; // even if a == null\n" +
                        "        '}'\n",
                        TypeUtils.getBoxedType(classB).getSimpleName()));
            }
        }

        final String nullA = nullForType(classA);
        final String nullB = nullForType(classB);

        sb.append(MessageFormat.format("" +
                "        if (a == {0}) '{'\n" +
                "            return (b == {1}) ? 0 : -1;\n" +
                "        '}'\n" +
                "        if (b == {1}) '{'\n" +
                "            return 1;\n" +
                "        '}'\n",
                nullA, nullB));

        // Handle special cases
        if (classA == BigDecimal.class || classB == BigDecimal.class) {
            // Always promote to BD
            final String a = maybePromote(classA, BigDecimal.class, "a");
            final String b = maybePromote(classB, BigDecimal.class, "b");
            sb.append(MessageFormat.format("" +
                    "        return {0}.compareTo({1});\n",
                    a, b));

        } else if ((classA == BigInteger.class && isFPNumber(classB))
                || (isFPNumber(classA) && classB == BigInteger.class)) {
            // upcast both to BigDecimal and compare
            final String a = maybePromote(classA, BigDecimal.class, "a");
            final String b = maybePromote(classB, BigDecimal.class, "b");
            sb.append(MessageFormat.format("" +
                    "        return {0}.compareTo({1});\n",
                    a, b));
        } else if ((classA == BigInteger.class || classB == BigInteger.class)) {
            // upcast both to BigInteger and compare
            final String a = maybePromote(classA, BigInteger.class, "a");
            final String b = maybePromote(classB, BigInteger.class, "b");
            sb.append(MessageFormat.format("" +
                    "        return {0}.compareTo({1});\n",
                    a, b));
        } else if (classA == long.class && isFPNumber(classB)) {
            // long must be compared as double
            sb.append("" +
                    "        if (Double.isNaN(b)) {\n" +
                    "            return -1;\n" +
                    "        }\n" +
                    "        if (b > Long.MAX_VALUE) {\n" +
                    "            return -1;\n" +
                    "        } else if (b < Long.MIN_VALUE) {\n" +
                    "            return 1;\n" +
                    "        } else {\n" +
                    "            final long longValue = (long) b;\n" +
                    "            if (longValue > a) {\n" +
                    "                return -1;\n" +
                    "            } else if (longValue == a) {\n" +
                    "                if (b - longValue == 0d) {\n" +
                    "                    return 0;\n" +
                    "                } else if (b - longValue > 0d) {\n" +
                    "                    return -1;\n" +
                    "                }\n" +
                    "            }\n" +
                    "            return 1;\n" +
                    "        }\n");
        } else if (isFPNumber(classA) && isFPNumber(classB)) {
            sb.append("        return Double.compare(a, b);\n");
        } else if (isFPNumber(classA) && isIntegralNumber(classB)) {
            final String boxType = TypeUtils.getBoxedType(classA).getSimpleName();
            sb.append(MessageFormat.format("" +
                    "        return {0}.compare(a, b);\n",
                    boxType));
        } else if (isIntegralNumber(classA) && isFPNumber(classB)) {
            final String boxType = TypeUtils.getBoxedType(classB).getSimpleName();
            sb.append(MessageFormat.format("" +
                    "        return {0}.compare(a, b);\n",
                    boxType));
        } else {
            sb.append("        return a < b ? -1 : (a == b ? 0 : 1);\n");
        }
        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate an eq() function for the given argument types.
     */
    private static String generateEqualityFunction(@NotNull final Class<?> classA, @NotNull final Class<?> classB) {
        final StringBuilder sb = new StringBuilder();

        sb.append(MessageFormat.format("    public static boolean eq({0} a, {1} b) '{'\n",
                classA.getSimpleName(),
                classB.getSimpleName()));

        if (isBigNumber(classA) || isBigNumber(classB)) {
            // NaN checks when converting to BigDecimal/BigInteger
            if (isFPNumber(classA)) {
                sb.append(MessageFormat.format("" +
                        "        if ({0}.isNaN(a)) '{'\n" +
                        "            return false;\n" +
                        "        '}'\n",
                        TypeUtils.getBoxedType(classA).getSimpleName()));
            }
            if (isFPNumber(classB)) {
                sb.append(MessageFormat.format("" +
                        "        if ({0}.isNaN(b)) '{'\n" +
                        "            return false;\n" +
                        "        '}'\n",
                        TypeUtils.getBoxedType(classB).getSimpleName()));
            }
        }

        final String nullA = nullForType(classA);
        final String nullB = nullForType(classB);

        sb.append(MessageFormat.format("" +
                "        if (a == {0}) '{'\n" +
                "            return b == {1};\n" +
                "        '}'\n" +
                "        if (b == {1}) '{'\n" +
                "            return false;\n" +
                "        '}'\n",
                nullA, nullB));

        if (isBigNumber(classA) || isBigNumber(classB)) {
            // promote to BD and compare
            final String a = maybePromote(classA, BigDecimal.class, "a");
            final String b = maybePromote(classB, BigDecimal.class, "b");
            sb.append(MessageFormat.format("" +
                    "        return {0}.compareTo({1}) == 0;\n",
                    a, b));
        } else {
            sb.append("        return a == b;\n");
        }
        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate inequality functions for the given argument types (e.g. less than, greater than, etc.).
     */
    private static String generateInequalityFunction(
            @NotNull final BinaryExpr.Operator op,
            @NotNull final Class<?> classA,
            @NotNull final Class<?> classB) {
        final StringBuilder sb = new StringBuilder();

        final String operatorName = QueryLanguageParser.getOperatorName(op);
        sb.append(MessageFormat.format("    public static boolean {0}({1} a, {2} b) '{'\n",
                operatorName,
                classA.getSimpleName(),
                classB.getSimpleName()));

        switch (op) {
            case LESS:
                sb.append("        return compareTo(a, b) < 0;\n");
                break;
            case GREATER:
                sb.append("        return compareTo(a, b) > 0;\n");
                break;
            case LESS_EQUALS:
                sb.append("        return compareTo(a, b) <= 0;\n");
                break;
            case GREATER_EQUALS:
                sb.append("        return compareTo(a, b) >= 0;\n");
                break;
            default:
                throw new IllegalStateException("Unsupported operator: " + op);
        }

        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate a null-safe cast function between the given argument types.
     */
    private static String generateCastFunction(@NotNull final Class<?> classA, @NotNull final Class<?> classB) {
        final StringBuilder sb = new StringBuilder();

        sb.append(MessageFormat.format("    public static {1} {1}Cast({0} a) '{'\n",
                classA.getSimpleName(),
                classB.getSimpleName()));

        final String nullA = nullForType(classA);
        final String nullB = nullForType(classB);

        sb.append(MessageFormat.format("" +
                "        return a == {0} ? {1} : ({2})a;\n",
                nullA, nullB, classB.getSimpleName()));
        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate a null-safe cast function from Object to the given primitive type.
     */
    private static String generateCastFromObjFunction(@NotNull final Class<?> classA) {
        final StringBuilder sb = new StringBuilder();

        final String typeA = classA.getSimpleName();
        sb.append(MessageFormat.format("    public static {0} {0}Cast(Object a) '{'\n",
                typeA));

        final String nullA = nullForType(classA);
        if (classA == char.class) {
            sb.append(MessageFormat.format("" +
                    "        return a == null ? {0} : (char)a;\n",
                    nullA));
        } else {
            sb.append(MessageFormat.format("" +
                    "        return a == null ? {0} : ((Number) a).{1}Value();\n",
                    nullA, typeA));
        }
        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate a negate() function for the given argument type.
     */
    private static String generateNegateFunction(@NotNull final Class<?> classA) {
        final StringBuilder sb = new StringBuilder();

        final Class<?> returnType;
        if (classA == int.class || classA == char.class || classA == byte.class || classA == short.class) {
            returnType = int.class;
        } else {
            returnType = classA;
        }

        sb.append(MessageFormat.format("    public static {0} negate({1} a) '{'\n",
                returnType.getSimpleName(), classA.getSimpleName()));

        final String nullA = nullForType(classA);
        final String nullReturn = nullForType(returnType);

        if (isBigNumber(classA)) {
            sb.append(MessageFormat.format("" +
                    "        return a == {0} ? {1} : a.negate();\n",
                    nullA, nullReturn));
        } else {

            sb.append(MessageFormat.format("" +
                    "        return a == {0} ? {1} : -a;\n",
                    nullA, nullReturn));
        }
        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate an array vs. array function for the given operator and argument types.
     */
    private static String generateArrayArrayFunction(
            @NotNull final BinaryExpr.Operator op,
            @NotNull final Class<?> classA,
            @NotNull final Class<?> classB,
            @NotNull final String opDescription) {
        final Class<?> returnType = getReturnType(op, classA, classB);

        final StringBuilder sb = new StringBuilder();

        final String operatorName = QueryLanguageParser.getOperatorName(op);
        sb.append(MessageFormat.format("    public static {0}[] {1}Array({2}[] a, {3}[] b) '{'\n",
                returnType.getSimpleName(),
                operatorName,
                classA.getSimpleName(),
                classB.getSimpleName()));

        sb.append(MessageFormat.format("" +
                "        if (a.length != b.length) '{'\n" +
                "            throw new IllegalArgumentException(\"Attempt to {4} two arrays ({2}, {3}) of different length (a.length=\" + a.length + \", b.length=\" + b.length + '')'');\n"
                +
                "        '}'\n" +
                "        {0}[] ret = new {0}[a.length];\n" +
                "        for (int i = 0; i < a.length; i++) '{'\n" +
                "            ret[i] = {1}(a[i], b[i]);\n" +
                "        '}'\n" +
                "\n" +
                "        return ret;\n",
                returnType.getSimpleName(),
                operatorName,
                classA.getSimpleName(),
                classB.getSimpleName(),
                opDescription));

        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate an array vs. variable function for the given operator and argument types.
     */
    private static String generateArrayVarFunction(
            @NotNull final BinaryExpr.Operator op,
            @NotNull final Class<?> classA,
            @NotNull final Class<?> classB) {
        final Class<?> returnType = getReturnType(op, classA, classB);

        final StringBuilder sb = new StringBuilder();

        final String operatorName = QueryLanguageParser.getOperatorName(op);
        sb.append(MessageFormat.format("    public static {0}[] {1}Array({2}[] a, {3} b) '{'\n",
                returnType.getSimpleName(),
                operatorName,
                classA.getSimpleName(),
                classB.getSimpleName()));

        sb.append(MessageFormat.format("" +
                "        {0}[] ret = new {0}[a.length];\n" +
                "        for (int i = 0; i < a.length; i++) '{'\n" +
                "            ret[i] = {1}(a[i], b);\n" +
                "        '}'\n" +
                "\n" +
                "        return ret;\n",
                returnType.getSimpleName(),
                operatorName));

        sb.append("    }\n\n");

        return sb.toString();
    }

    /**
     * Generate a variable vs. array function for the given operator and argument types.
     */
    private static String generateVarArrayFunction(
            @NotNull final BinaryExpr.Operator op,
            @NotNull final Class<?> classA,
            @NotNull final Class<?> classB) {
        final Class<?> returnType = getReturnType(op, classA, classB);

        final StringBuilder sb = new StringBuilder();

        final String operatorName = QueryLanguageParser.getOperatorName(op);
        sb.append(MessageFormat.format("    public static {0}[] {1}Array({2} a, {3}[] b) '{'\n",
                returnType.getSimpleName(),
                operatorName,
                classA.getSimpleName(),
                classB.getSimpleName()));

        sb.append(MessageFormat.format("" +
                "        {0}[] ret = new {0}[b.length];\n" +
                "        for (int i = 0; i < b.length; i++) '{'\n" +
                "            ret[i] = {1}(a, b[i]);\n" +
                "        '}'\n" +
                "\n" +
                "        return ret;\n",
                returnType.getSimpleName(),
                operatorName));

        sb.append("    }\n\n");

        return sb.toString();
    }

    // endregion Generate Functions

    // region Helper Functions

    private static Class<?> getReturnType(
            @NotNull final BinaryExpr.Operator op,
            @NotNull Class<?> classA,
            @NotNull Class<?> classB) {
        if (op == BinaryExpr.Operator.EQUALS || op == BinaryExpr.Operator.LESS || op == BinaryExpr.Operator.GREATER
                || op == BinaryExpr.Operator.LESS_EQUALS || op == BinaryExpr.Operator.GREATER_EQUALS) {
            return boolean.class;
        } else if (TypeUtils.getBoxedType(classA) == Boolean.class
                || TypeUtils.getBoxedType(classB) == Boolean.class) {
            return Boolean.class;
        } else if (isBigNumber(classA) || isBigNumber(classB)) {
            return bigDecimalIntegerPromotionType(classA, classB, op);
        } else if (op == BinaryExpr.Operator.DIVIDE && isIntegralNumber(classB)) {
            return double.class;
        }
        return QueryLanguageParser.binaryNumericPromotionType(classA, classB);
    }

    private static String nullForType(Class<?> type) {
        if (type == boolean.class) {
            return "QueryConstants.NULL_BOOLEAN";
        } else if (type == byte.class) {
            return "QueryConstants.NULL_BYTE";
        } else if (type == char.class) {
            return "QueryConstants.NULL_CHAR";
        } else if (type == short.class) {
            return "QueryConstants.NULL_SHORT";
        } else if (type == int.class) {
            return "QueryConstants.NULL_INT";
        } else if (type == long.class) {
            return "QueryConstants.NULL_LONG";
        } else if (type == float.class) {
            return "QueryConstants.NULL_FLOAT";
        } else if (type == double.class) {
            return "QueryConstants.NULL_DOUBLE";
        } else {
            return "null";
        }
    }

    private static String maybePromote(Class<?> fromType, Class<?> toType, String varName) {
        if (fromType == toType) {
            return varName;
        }
        if (toType == BigDecimal.class) {
            if (fromType == BigInteger.class) {
                return "(new BigDecimal(" + varName + "))";
            } else {
                return "BigDecimal.valueOf(" + varName + ")";
            }
        }
        if (toType == BigInteger.class) {
            return "BigInteger.valueOf(" + varName + ")";
        }
        return "((" + toType.getSimpleName() + ") " + varName + ")";
    }

    private static String operatorStringForTypes(Class<?> type1, Class<?> type2, Class<?> promotedType,
            BinaryExpr.Operator op) {
        final String a = maybePromote(type1, promotedType, "a");
        final String b = maybePromote(type2, promotedType, "b");

        if (promotedType == BigDecimal.class || promotedType == BigInteger.class) {
            final MessageFormat formatString;

            switch (op) {
                case PLUS:
                    formatString = new MessageFormat("{0}.add({1})");
                    break;
                case MINUS:
                    formatString = new MessageFormat("{0}.subtract({1})");
                    break;
                case MULTIPLY:
                    formatString = new MessageFormat("{0}.multiply({1})");
                    break;
                case DIVIDE:
                    // this looks sketchy but DIVIDE requires BigDecimal promotion
                    formatString = new MessageFormat(
                            "{0}.divide({1}, max(max({0}.scale(), {1}.scale()), DEFAULT_SCALE), ROUNDING_MODE)");
                    break;
                case REMAINDER:
                    formatString = new MessageFormat("{0}.remainder({1})");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator for BigDecimal promotion: " + op);
            }
            return formatString.format(new Object[] {a, b});
        }
        return a + " " + QueryLanguageParser.getOperatorSymbol(op) + " " + b;
    }

    private static Class<?> bigDecimalIntegerPromotionType(Class<?> type1, Class<?> type2, BinaryExpr.Operator op) {
        // one of the types must be BigDecimal or BigInteger
        assert type1 == BigDecimal.class || type1 == BigInteger.class
                || type2 == BigDecimal.class || type2 == BigInteger.class;

        if (op == null) {
            return int.class; // is a comparison operation
        }

        if (type1 == BigDecimal.class || type2 == BigDecimal.class || op == BinaryExpr.Operator.DIVIDE) {
            return BigDecimal.class; // never downcast
        }

        // We know one at least one of these is BigInteger, what is the other type?
        final Class<?> otherType = type1 == BigInteger.class ? type2 : type1;

        switch (op) {
            case PLUS:
            case MINUS:
            case MULTIPLY:
            case REMAINDER:
                if (otherType == float.class || otherType == double.class) {
                    return BigDecimal.class;
                }
                return BigInteger.class;
            default:
                throw new IllegalArgumentException("Unsupported operator for BigInteger/BigDecimal promotion: " + op);
        }
    }

    private static boolean isIntegralNumber(Class<?> type) {
        type = TypeUtils.getUnboxedType(type);

        // noinspection SimplifiableIfStatement
        if (type == null) {
            return false;
        }

        return type == int.class || type == long.class || type == byte.class || type == short.class
                || type == char.class;
    }

    private static boolean isFPNumber(Class<?> type) {
        type = TypeUtils.getUnboxedType(type);

        // noinspection SimplifiableIfStatement
        if (type == null) {
            return false;
        }

        return type == double.class || type == float.class;
    }

    private static boolean isBigNumber(Class<?> type) {
        return type == BigDecimal.class || type == BigInteger.class;
    }

    // endregion Helper Functions
}
