//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replication;

import io.deephaven.base.verify.Require;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.deephaven.replication.ReplicationUtils.className;
import static io.deephaven.replication.ReplicationUtils.findNoLocateRegions;
import static io.deephaven.replication.ReplicationUtils.replaceRegion;

public class ReplicatePrimitiveCode {

    private static String replicateCodeBasedOnChar(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace, String upperCharReplace,
            String characterReplace, String charReplace, String allCapsCharReplace) throws IOException {
        String[][] pairs = new String[][] {
                {"Character", upperCharacterReplace},
                {"Char", upperCharReplace},
                {"char", charReplace},
                {"character", characterReplace},
                {"CHAR", allCapsCharReplace}
        };
        return replaceAll(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    private static String replicateCodeBasedOnInt(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace, String upperCharReplace,
            String characterReplace, String charReplace, String allCapsCharReplace) throws IOException {
        String[][] pairs = new String[][] {
                {"Integer", upperCharacterReplace},
                {"Int", upperCharReplace},
                {"integer", characterReplace},
                {"int", charReplace},
                {"INT", allCapsCharReplace}
        };
        return replaceAll(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    private static String replicateCodeBasedOnShort(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
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
        return replaceAll(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    private static void replicateCodeBasedOnFloat(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String[] exemptions, String upperCharacterReplace,
            String charReplace, String allCapsCharReplace) throws IOException {
        String[][] pairs = new String[][] {
                {"Float", upperCharacterReplace},
                {"float", charReplace},
                {"FLOAT", allCapsCharReplace}
        };
        replaceAll(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    public static String charToBoolean(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Boolean", "Boolean", "boolean", "boolean", "BOOLEAN");
    }

    public static String charToBooleanAsByte(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Boolean", "Boolean", "boolean", "byte", "BOOLEAN");
    }

    private static String charToObject(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Object", "Object", "Object", "Object", "OBJECT");
    }

    public static String charToByte(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Byte", "Byte", "byte", "byte", "BYTE");
    }

    public static String charToDouble(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Double", "Double", "double", "double", "DOUBLE");
    }

    public static String charToFloat(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Float", "Float", "float", "float", "FLOAT");
    }

    public static String charToInteger(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Integer", "Int", "integer", "int", "INT");
    }

    public static String charToLong(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Long", "Long", "long", "long", "LONG");
    }

    public static String charToInstant(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Instant", "Instant", "Instant", "Instant", "INSTANT");
    }

    public static String charToShort(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Short", "Short", "short", "short", "SHORT");
    }

    public static String charLongToLongInt(String gradleTask, String sourceClassJavaPath, String... exemptions)
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
        return replaceAll(gradleTask, sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String longToInt(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        final String[][] pairs = new String[][] {
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't
                // actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort
                // kernel that needs it.
                {"Long", "Int"},
                {"long", "int"},
                {"LONG", "INT"},
        };
        return replaceAll(gradleTask, sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String longToByte(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        final String[][] pairs = new String[][] {
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't
                // actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort
                // kernel that needs it.
                {"Long", "Byte"},
                {"long", "byte"},
                {"LONG", "BYTE"},
        };
        return replaceAll(gradleTask, sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String charLongToIntInt(String gradleTask, String sourceClassJavaPath, String... exemptions)
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
        return replaceAll(gradleTask, sourceClassJavaPath, null, exemptions, pairs);
    }

    public static String intToObject(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, null, exemptions,
                "Object", "Object", "Object", "Object", "OBJECT");
    }

    private static String intToObject(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Object", "Object", "Object", "Object", "OBJECT");
    }

    public static String intToChar(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Character", "Char", "char", "char", "CHAR");
    }

    public static String intToByte(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Byte", "Byte", "byte", "byte", "BYTE");
    }

    public static String intToDouble(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Double", "Double", "double", "double", "DOUBLE");
    }

    public static String intToFloat(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Float", "Float", "float", "float", "FLOAT");
    }

    public static String intToLong(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Long", "Long", "long", "long", "LONG");
    }

    public static String intToShort(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions,
                "Short", "Short", "short", "short", "SHORT");
    }

    private static String shortToByte(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, "Byte", "byte",
                "BYTE");
    }

    private static String shortToDouble(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, "Double",
                "double",
                "DOUBLE");
    }

    private static String shortToFloat(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, "Float",
                "float", "FLOAT");
    }

    private static String shortToInteger(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, "Integer",
                "int", "INT",
                new String[][] {{"ShortVector", "IntVector"}});
    }

    private static String shortToLong(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        return replicateCodeBasedOnShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, "Long", "long",
                "LONG",
                new String[][] {{"Integer.signum", "Long.signum"}});
    }

    private static void floatToDouble(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        replicateCodeBasedOnFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions, "Double", "double",
                "DOUBLE");
    }

    public static List<String> charToAll(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToAll(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    private static List<String> charToAll(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(charToBoolean(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToInteger(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(charToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        return results;
    }

    public static List<String> charToIntegers(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToIntegers(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static List<String> charToAllButBoolean(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToAllButBoolean(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static String charToByte(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToByte(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static String charToObject(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToObject(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static String charToBoolean(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToBoolean(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static String charToLong(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToLong(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static String charToInstant(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return charToInstant(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static void charToAllButBooleanAndLong(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        charToAllButBooleanAndLong(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static List<String> charToAllButBoolean(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> resultFiles = new ArrayList<>();
        resultFiles.add(charToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToInteger(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));

        return resultFiles;
    }

    public static List<String> charToIntegers(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> resultFiles = new ArrayList<>();
        resultFiles.add(charToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToInteger(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        resultFiles.add(charToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        return resultFiles;
    }

    private static void charToAllButBooleanAndLong(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        charToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToInteger(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        charToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static List<String> charToAllButBooleanAndByte(String gradleTask, String sourceClassJavaPath,
            String... exemptions)
            throws IOException {
        return charToAllButBooleanAndByte(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    private static List<String> charToAllButBooleanAndByte(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> files = new ArrayList<>();
        files.add(charToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(charToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(charToInteger(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(charToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(charToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        return files;
    }

    public static List<String> charToAllButBooleanAndFloats(String gradleTask, String sourceClass, String... exemptions)
            throws IOException {
        return charToAllButBooleanAndFloats(gradleTask, sourceClass, null, exemptions);
    }

    public static void charToShortAndByte(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        charToByte(gradleTask, sourceClassJavaPath, null, exemptions);
        charToShort(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static List<String> charToAllButBooleanAndFloats(String gradleTask, String sourceClass,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> files = new ArrayList<>();
        files.add(charToInteger(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        files.add(charToByte(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        files.add(charToLong(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        files.add(charToShort(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        return files;
    }

    public static List<String> shortToAllNumericals(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(shortToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(shortToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(shortToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(shortToInteger(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(shortToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        return results;
    }

    public static void intToAllNumericals(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        intToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
        intToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    public static List<String> intToAllButBoolean(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        return intToAllButBoolean(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static List<String> intToAllButBoolean(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(intToChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        return results;
    }

    public static List<String> intToAllButBooleanAndLong(String gradleTask, String sourceClassJavaPath,
            String... exemptions) throws IOException {
        return intToAllButBooleanAndLong(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    public static List<String> intToAllButBooleanAndLong(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(intToChar(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToByte(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToShort(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        results.add(intToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        return results;
    }

    public static List<String> shortToAllIntegralTypes(String gradleTask, String sourceClass, String... exemptions)
            throws IOException {
        return shortToAllIntegralTypes(gradleTask, sourceClass, null, exemptions);
    }

    private static List<String> shortToAllIntegralTypes(String gradleTask, String sourceClass,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(shortToByte(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        results.add(shortToInteger(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        results.add(shortToLong(gradleTask, sourceClass, serialVersionUIDs, exemptions));
        return results;
    }

    public static void floatToAllFloatingPoints(String gradleTask, String sourceClassJavaPath, String... exemptions)
            throws IOException {
        floatToAllFloatingPoints(gradleTask, sourceClassJavaPath, null, exemptions);
    }

    private static void floatToAllFloatingPoints(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        floatToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions);
    }

    private static String getResultClassPathFromSourceClassPath(String sourceClassJavaPath,
            String[] exemptions, String[]... pairs) {
        final String sourceClassName = ReplicationUtils.className(sourceClassJavaPath);
        final String resultClassName = replaceAllInternal(sourceClassName, null, exemptions, pairs);
        final String resultClassJavaPath = basePath(sourceClassJavaPath) + '/' + resultClassName + ".java";
        return resultClassJavaPath;
    }

    public static String replaceAll(String gradleTask, String sourceClassJavaPath, Map<String, Long> serialVersionUIDs,
            String[] exemptions, String[]... pairs) throws IOException {
        final String resultClassJavaPath =
                getResultClassPathFromSourceClassPath(sourceClassJavaPath, exemptions, pairs);
        return replaceAll(gradleTask, sourceClassJavaPath, resultClassJavaPath, serialVersionUIDs, exemptions, pairs);
    }

    public static String replaceAll(String gradleTask, String sourceClassJavaPath, String resultClassJavaPath,
            Map<String, Long> serialVersionUIDs, String[] exemptions, String[]... pairs) throws IOException {
        if (resultClassJavaPath == null || resultClassJavaPath.isEmpty()) {
            resultClassJavaPath = getResultClassPathFromSourceClassPath(sourceClassJavaPath, exemptions, pairs);
        }
        final String resultClassName = ReplicationUtils.className(resultClassJavaPath);
        final String resultClassPackageName = packageName(resultClassJavaPath);
        final String fullResultClassName = resultClassPackageName + '.' + resultClassName;
        final Long serialVersionUID = serialVersionUIDs == null ? null : serialVersionUIDs.get(fullResultClassName);

        final InputStream inputStream = new FileInputStream(sourceClassJavaPath);
        int nextChar;
        final StringBuilder inputText = new StringBuilder();
        while ((nextChar = inputStream.read()) != -1) {
            inputText.append((char) nextChar);
        }
        inputStream.close();

        System.out.println("Generating java file " + resultClassJavaPath);
        String body = replaceAllInternal(inputText.toString(), serialVersionUID, exemptions, pairs);
        final Map<String, List<String>> noReplicateParts = findNoLocateRegions(resultClassJavaPath);
        if (!noReplicateParts.isEmpty()) {
            final StringReader sr = new StringReader(body);
            List<String> lines = IOUtils.readLines(sr);
            for (Map.Entry<String, List<String>> ent : noReplicateParts.entrySet()) {
                lines = replaceRegion(lines, "@NoReplicate " + ent.getKey(), ent.getValue());
            }
            body = String.join("\n", lines);
        }

        PrintWriter out = new PrintWriter(resultClassJavaPath);

        // Remove the first comment block of the file, pruning prev copyright header (and sometimes generated header) so
        // we write our own
        while (body.startsWith("//")) {
            body = body.substring(body.indexOf("\n", 2) + 1);
        }

        out.print(ReplicationUtils.fileHeaderString(gradleTask, className(sourceClassJavaPath)));

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
    public static String fullClassName(@NotNull final String sourceClassJavaPath) {
        return packageName(sourceClassJavaPath) + '.' + ReplicationUtils.className(sourceClassJavaPath);
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

    public static List<String> intToLongAndFloatingPoints(String gradleTask, String sourceClassJavaPath,
            Map<String, Long> serialVersionUIDs,
            String... exemptions) throws IOException {
        final List<String> files = new ArrayList<>();
        files.add(intToDouble(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(intToFloat(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));
        files.add(intToLong(gradleTask, sourceClassJavaPath, serialVersionUIDs, exemptions));

        return files;
    }

    public static List<String> intToLongAndFloatingPoints(String gradleTask, String sourceClassJavaPath,
            String... exemptions)
            throws IOException {
        return intToLongAndFloatingPoints(gradleTask, sourceClassJavaPath, null, exemptions);
    }
}
