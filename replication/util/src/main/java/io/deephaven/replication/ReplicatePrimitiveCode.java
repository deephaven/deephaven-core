/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replication;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReplicatePrimitiveCode {

    private static String replicateCodeBasedOnChar(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace, String upperCharReplace,
            String characterReplace, String charReplace, String allCapsCharReplace) throws IOException {
        String[][] pairs = new String[][] {
                {"Character", upperCharacterReplace},
                {"Char", upperCharReplace},
                {"char", charReplace},
                {"character", characterReplace},
                {"CHAR", allCapsCharReplace}
        };
        return replaceAll(sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    private static String replicateCodeBasedOnInt(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace, String upperCharReplace,
            String characterReplace, String charReplace, String allCapsCharReplace) throws IOException {
        String[][] pairs = new String[][] {
                {"Integer", upperCharacterReplace},
                {"Int", upperCharReplace},
                {"integer", characterReplace},
                {"int", charReplace},
                {"INT", allCapsCharReplace}
        };
        return replaceAll(sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    private static String replicateCodeBasedOnShort(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace,
            String charReplace, String allCapsCharReplace, String[]... extraPairs) throws IOException {
        final String[][] pairs;
        final int extraPairsLength;
        if (extraPairs != null) {
            extraPairsLength = extraPairs.length;
            pairs = new String[extraPairsLength + 3][];
            System.arraycopy(extraPairs, 0, pairs, 0, extraPairs.length);
        } else {
            extraPairsLength = 0;
            pairs = new String[3][];
        }
        pairs[extraPairsLength] = new String[] {"Short", upperCharacterReplace};
        pairs[extraPairsLength + 1] = new String[] {"short", charReplace};
        pairs[extraPairsLength + 2] = new String[] {"SHORT", allCapsCharReplace};
        return replaceAll(sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    private static void replicateCodeBasedOnFloat(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace,
            String charReplace, String allCapsCharReplace) throws IOException {
        String[][] pairs = new String[][] {
                {"Float", upperCharacterReplace},
                {"float", charReplace},
                {"FLOAT", allCapsCharReplace}
        };
        replaceAll(sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    public static String charToBoolean(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Boolean", "Boolean", "boolean", "boolean", "BOOLEAN");
    }

    public static String charToBooleanAsByte(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Boolean", "Boolean", "boolean", "byte", "BOOLEAN");
    }

    private static String charToObject(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Object", "Object", "Object", "Object", "OBJECT");
    }

    public static String charToByte(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Byte", "Byte", "byte", "byte", "BYTE");
    }

    public static String charToDouble(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Double", "Double", "double", "double", "DOUBLE");
    }

    public static String charToFloat(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Float", "Float", "float", "float", "FLOAT");
    }

    public static String charToInteger(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Integer", "Int", "integer", "int", "INT");
    }

    public static String charToLong(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Long", "Long", "long", "long", "LONG");
    }

    public static String charToShort(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Short", "Short", "short", "short", "SHORT");
    }

    public static String charLongToLongInt(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        final String[][] pairs = new String[][] {
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't
                // actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort
                // kernel that needs it.
                {"Long", "Int"},
                {"long", "int"},
                {"LONG", "INT"},
                {"Character", "Long"},
                {"Char", "Long"},
                {"char", "long"},
                {"character", "long"},
                {"CHAR", "LONG"}
        };
        return replaceAll(sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String longToInt(String sourceClassJavaPath, String... exemptions) throws IOException {
        final String[][] pairs = new String[][] {
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't
                // actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort
                // kernel that needs it.
                {"Long", "Int"},
                {"long", "int"},
                {"LONG", "INT"},
        };
        return replaceAll(sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String charLongToIntInt(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        final String[][] pairs = new String[][] {
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't
                // actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort
                // kernel that needs it.
                {"Long", "Int"},
                {"long", "int"},
                {"LONG", "INT"},
                {"Character", "Integer"},
                {"Char", "Int"},
                {"char", "int"},
                {"character", "int"},
                {"CHAR", "INT"}
        };
        return replaceAll(sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String intToObject(String sourceClassJavaPath, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, null, exemptions,
                "Object", "Object", "Object", "Object", "OBJECT");
    }

    private static String intToObject(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Object", "Object", "Object", "Object", "OBJECT");
    }

    private static String intToChar(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Character", "Char", "char", "char", "CHAR");
    }

    private static String intToByte(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Byte", "Byte", "byte", "byte", "BYTE");
    }

    public static String intToDouble(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Double", "Double", "double", "double", "DOUBLE");
    }

    private static String intToFloat(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Float", "Float", "float", "float", "FLOAT");
    }

    public static String intToLong(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Long", "Long", "long", "long", "LONG");
    }

    private static String intToShort(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Short", "Short", "short", "short", "SHORT");
    }

    private static String shortToByte(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(sourceClassJavaPath, serialVersionUIDs, exemptions, "Byte", "byte", "BYTE");
    }

    private static void shortToDouble(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClassJavaPath, serialVersionUIDs, exemptions, "Double", "double", "DOUBLE");
    }

    private static void shortToFloat(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClassJavaPath, serialVersionUIDs, exemptions, "Float", "float", "FLOAT");
    }

    private static String shortToInteger(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(sourceClassJavaPath, serialVersionUIDs, exemptions, "Integer", "int", "INT",
                new String[][] {{"ShortVector", "IntVector"}});
    }

    private static String shortToLong(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(sourceClassJavaPath, serialVersionUIDs, exemptions, "Long", "long", "LONG",
                new String[][] {{"Integer.signum", "Long.signum"}});
    }

    private static void floatToDouble(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        replicateCodeBasedOnFloat(sourceClassJavaPath, serialVersionUIDs, exemptions, "Double", "double", "DOUBLE");
    }

    public static List<String> charToAll(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToAll(sourceClassJavaPath, null, exemptions);
    }

    private static List<String> charToAll(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(charToBoolean(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToByte(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToInteger(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToLong(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToShort(sourceClassJavaPath, serialVersionUIDs, exemptions));
        return results;
    }

    public static List<String> charToIntegers(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToIntegers(sourceClassJavaPath, null, exemptions);
    }

    public static List<String> charToAllButBoolean(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToAllButBoolean(sourceClassJavaPath, null, exemptions);
    }

    public static String charToByte(String sourceClassJavaPath, String... exemptions) throws IOException {
        return charToByte(sourceClassJavaPath, null, exemptions);
    }

    public static String charToObject(String sourceClassJavaPath, String... exemptions) throws IOException {
        return charToObject(sourceClassJavaPath, null, exemptions);
    }

    public static String charToBoolean(String sourceClassJavaPath, String... exemptions) throws IOException {
        return charToBoolean(sourceClassJavaPath, null, exemptions);
    }

    public static String charToLong(String sourceClassJavaPath, String... exemptions) throws IOException {
        return charToLong(sourceClassJavaPath, null, exemptions);
    }

    public static void charToAllButBooleanAndLong(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        charToAllButBooleanAndLong(sourceClassJavaPath, null, exemptions);
    }

    public static List<String> charToAllButBoolean(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> resultFiles = new ArrayList<>();
        resultFiles.add(charToByte(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToInteger(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToLong(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToShort(sourceClassJavaPath, serialVersionUIDs, exemptions));

        return resultFiles;
    }

    public static List<String> charToIntegers(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> resultFiles = new ArrayList<>();
        resultFiles.add(charToByte(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToShort(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToInteger(sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToLong(sourceClassJavaPath, serialVersionUIDs, exemptions));
        return resultFiles;
    }

    private static void charToAllButBooleanAndLong(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        charToByte(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToInteger(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToShort(sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static void charToAllButBooleanAndByte(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        charToAllButBooleanAndByte(sourceClassJavaPath, null, exemptions);
    }

    private static void charToAllButBooleanAndByte(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        charToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToInteger(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToLong(sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToShort(sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static List<String> charToAllButBooleanAndFloats(String sourceClass, String... exemptions)
            throws IOException {
        return charToAllButBooleanAndFloats(sourceClass, null, exemptions);
    }

    public static void charToShortAndByte(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        charToByte(sourceClassJavaPath, null, exemptions);
        charToShort(sourceClassJavaPath, null, exemptions);
    }

    public static List<String> charToAllButBooleanAndFloats(String sourceClass, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> files = new ArrayList<>();
        files.add(charToInteger(sourceClass, serialVersionUIDs, exemptions));
        files.add(charToByte(sourceClass, serialVersionUIDs, exemptions));
        files.add(charToLong(sourceClass, serialVersionUIDs, exemptions));
        files.add(charToShort(sourceClass, serialVersionUIDs, exemptions));
        return files;
    }

    public static void shortToAllNumericals(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        shortToByte(sourceClassJavaPath, serialVersionUIDs, exemptions);
        shortToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions);
        shortToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions);
        shortToInteger(sourceClassJavaPath, serialVersionUIDs, exemptions);
        shortToLong(sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static void intToAllNumericals(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        intToByte(sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToLong(sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToShort(sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static List<String> intToAllButBoolean(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return intToAllButBoolean(sourceClassJavaPath, null, exemptions);
    }

    public static List<String> intToAllButBoolean(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(intToChar(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToByte(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToLong(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToShort(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions));
        return results;
    }

    public static List<String> shortToAllIntegralTypes(String sourceClass, String... exemptions) throws IOException {
        return shortToAllIntegralTypes(sourceClass, null, exemptions);
    }

    private static List<String> shortToAllIntegralTypes(String sourceClass, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(shortToByte(sourceClass, serialVersionUIDs, exemptions));
        results.add(shortToInteger(sourceClass, serialVersionUIDs, exemptions));
        results.add(shortToLong(sourceClass, serialVersionUIDs, exemptions));
        return results;
    }

    public static void floatToAllFloatingPoints(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        floatToAllFloatingPoints(sourceClassJavaPath, null, exemptions);
    }

    private static void floatToAllFloatingPoints(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        floatToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static String replaceAll(String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String[] exemptions, String[]... pairs) throws IOException {
        final InputStream inputStream = new FileInputStream(sourceClassJavaPath);
        int nextChar;
        final StringBuilder inputText = new StringBuilder();
        while ((nextChar = inputStream.read()) != -1) {
            inputText.append((char) nextChar);
        }
        inputStream.close();

        final String sourceClassName = className(sourceClassJavaPath);
        final String packageName = packageName(sourceClassJavaPath);

        final String resultClassName = replaceAllInternal(sourceClassName, null, exemptions, pairs);
        final String fullResultClassName = packageName + '.' + resultClassName;
        final Long serialVersionUID = serialVersionUIDs == null ? null : serialVersionUIDs.get(fullResultClassName);
        final String resultClassJavaPath = basePath(sourceClassJavaPath) + '/' + resultClassName + ".java";


        System.out.println("Generating java file " + resultClassJavaPath);
        PrintWriter out = new PrintWriter(resultClassJavaPath);

        String body = replaceAllInternal(inputText.toString(), serialVersionUID, exemptions, pairs);

        // preserve the first comment of the file; typically the copyright
        if (body.startsWith("/*")) {
            final int idx = body.indexOf("*/");
            if (idx != -1 && body.length() >= idx + 3) {
                out.print(body.substring(0, idx + 3));
                body = body.substring(idx + 3);
            }
        }

        out.println("/*");
        out.println(
                " * ---------------------------------------------------------------------------------------------------------------------");
        out.println(" * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit "
                + sourceClassName + " and regenerate");
        out.println(
                " * ---------------------------------------------------------------------------------------------------------------------");
        out.println(
                " */");

        out.print(body);
        out.flush();
        out.close();

        return resultClassJavaPath;
    }

    @NotNull
    public static String basePath(@NotNull final String sourceClassJavaPath) {
        Require.requirement(sourceClassJavaPath.endsWith(".java"),
                "sourceClassJavaPath.endsWith(\".java\")");
        return new File(sourceClassJavaPath).getParent();
    }

    @NotNull
    public static String javaFileName(@NotNull final String sourceClassJavaPath) {
        Require.requirement(sourceClassJavaPath.endsWith(".java"),
                "sourceClassJavaPath.endsWith(\".java\")");
        return new File(sourceClassJavaPath).getName();
    }

    @NotNull
    public static String packageName(@NotNull final String sourceClassJavaPath) {
        final String basePath = basePath(sourceClassJavaPath);
        final int indexOfJava = basePath.indexOf("/java/");
        Require.requirement(indexOfJava >= 0, "source class java file path contains \"/java/\"");
        final String packagePath = basePath.substring(indexOfJava + "/java/".length());
        return packagePath.replace('/', '.');
    }

    @NotNull
    public static String className(@NotNull final String sourceClassJavaPath) {
        final String javaFileName = javaFileName(sourceClassJavaPath);
        return javaFileName.substring(0, javaFileName.length() - ".java".length());
    }

    @NotNull
    public static String fullClassName(@NotNull final String sourceClassJavaPath) {
        return packageName(sourceClassJavaPath) + '.' + className(sourceClassJavaPath);
    }

    public static String replaceAllInternal(String inputText, Long serialVersionUID, String[] exemptions,
            String[]... pairs) {
        String result = inputText;
        for (int i = 0; i < exemptions.length; i++) {
            String exemption = exemptions[i];
            result = result.replaceAll(exemption, "@_@_exemption" + i + "_@_@");
        }
        for (int i = 0; i < pairs.length; i++) {
            String[] pair = pairs[i];
            result = result.replaceAll(pair[0], "@_@_pair" + i + "_@_@");
        }
        for (int i = 0; i < pairs.length; i++) {
            String[] pair = pairs[i];
            result = result.replaceAll("@_@_pair" + i + "_@_@", pair[1]);
        }
        for (int i = 0; i < exemptions.length; i++) {
            String exemption = exemptions[i];
            result = result.replaceAll("@_@_exemption" + i + "_@_@", exemption);
        }

        if (serialVersionUID != null) {
            result = result.replaceAll(
                    "(\\s+(private\\s+)?(static\\s+)?(final\\s+)?long\\s+serialVersionUID\\s+=\\s+)\\-?[0-9]+L\\s*;",
                    "$1" + serialVersionUID + "L;");
        }

        return result;
    }

    public static List<String> intToLongAndFloatingPoints(String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> files = new ArrayList<>();
        files.add(intToDouble(sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(intToFloat(sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(intToLong(sourceClassJavaPath, serialVersionUIDs, exemptions));

        return files;
    }

    public static List<String> intToLongAndFloatingPoints(String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return intToLongAndFloatingPoints(sourceClassJavaPath, null, exemptions);
    }
}
