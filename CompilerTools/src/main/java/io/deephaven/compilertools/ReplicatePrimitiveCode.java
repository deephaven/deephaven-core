/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.compilertools;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReplicatePrimitiveCode {

    public static final String MAIN_SRC = "src/main/java";
    public static final String TEST_SRC = "src/test/java";
    public static final String BENCHMARK_SRC = "benchmark";

    private static String replicateCodeBasedOnChar(Class sourceClass, Map<String, Long> serialVersionUIDs, String exemptions[], String upperCharacterReplace, String upperCharReplace,
                                                   String characterReplace, String charReplace, String root, String allCapsCharReplace) throws IOException {
        String pairs[][] = new String[][]{
                {"Character",upperCharacterReplace},
                {"Char",upperCharReplace},
                {"char",charReplace},
                {"character",characterReplace},
                {"CHAR",allCapsCharReplace}
        };
        return replaceAll(sourceClass, serialVersionUIDs, exemptions, root, pairs);
    }

    private static String replicateCodeBasedOnInt(Class sourceClass, Map<String, Long> serialVersionUIDs, String exemptions[], String upperCharacterReplace, String upperCharReplace,
                                                String characterReplace, String charReplace, String root, String allCapsCharReplace) throws IOException {
        String pairs[][] = new String[][]{
                {"Integer",upperCharacterReplace},
                {"Int",upperCharReplace},
                {"integer",characterReplace},
                {"int",charReplace},
                {"INT",allCapsCharReplace}
        };
        return replaceAll(sourceClass, serialVersionUIDs, exemptions, root, pairs);
    }

    private static void replicateCodeBasedOnShort(Class sourceClass, Map<String, Long> serialVersionUIDs, String exemptions[], String upperCharacterReplace,
                                                  String charReplace, String root, String allCapsCharReplace, String[]... extraPairs) throws IOException {
        final String pairs[][];
        final int extraPairsLength;
        if (extraPairs != null) {
            extraPairsLength = extraPairs.length;
            pairs = new String[extraPairsLength + 3][];
            System.arraycopy(extraPairs, 0, pairs, 0, extraPairs.length);
        } else {
            extraPairsLength = 0;
            pairs = new String[3][];
        }
        pairs[extraPairsLength] = new String[]{"Short", upperCharacterReplace};
        pairs[extraPairsLength + 1] = new String[]{"short", charReplace};
        pairs[extraPairsLength + 2] = new String[]{"SHORT", allCapsCharReplace};
        replaceAll(sourceClass, serialVersionUIDs, exemptions, root, pairs);
    }

    private static void replicateCodeBasedOnFloat(Class sourceClass, Map<String, Long> serialVersionUIDs, String exemptions[], String upperCharacterReplace,
                                                  String charReplace, String root, String allCapsCharReplace) throws IOException {
        String pairs[][] = new String[][]{
                {"Float",upperCharacterReplace},
                {"float",charReplace},
                {"FLOAT",allCapsCharReplace}
        };
        replaceAll(sourceClass, serialVersionUIDs, exemptions, root, pairs);
    }

    public static String charToBoolean(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions, "Boolean", "Boolean", "boolean", "boolean", root, "BOOLEAN");
    }

    public static String charToBooleanAsByte(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions, "Boolean", "Boolean", "boolean", "byte", root, "BOOLEAN");
    }

    private static String charToObject(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions, "Object", "Object", "Object", "Object", root, "OBJECT");
    }

    private static String charToByte(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions, "Byte", "Byte", "byte", "byte", root, "BYTE");
    }

    public static String charToDouble(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions,"Double","Double","double","double", root, "DOUBLE");
    }

    public static String charToFloat(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions,"Float","Float","float","float", root, "FLOAT");
    }

    public static String charToInteger(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions,"Integer","Int","integer","int", root, "INT");
    }

    public static String charToLong(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions,"Long","Long","long","long", root, "LONG");
    }

    private static String charToShort(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnChar(sourceClass, serialVersionUIDs, exemptions, "Short", "Short", "short", "short", root, "SHORT");
    }

    public static String charLongToLongInt(Class sourceClass, String root, String... exemptions) throws IOException {
        final String pairs[][] = new String[][]{
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort kernel that needs it.
                {"Long","Int"},
                {"long","int"},
                {"LONG","INT"},
                {"Character","Long"},
                {"Char","Long"},
                {"char","long"},
                {"character","long"},
                {"CHAR","LONG"}
        };
        return replaceAll(sourceClass, null, exemptions, root, pairs);
    }

    public static String longToInt(Class sourceClass, String root, String... exemptions) throws IOException {
        final String pairs[][] = new String[][]{
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort kernel that needs it.
                {"Long","Int"},
                {"long","int"},
                {"LONG","INT"},
        };
        return replaceAll(sourceClass, null, exemptions, root, pairs);
    }

    public static String charLongToIntInt(Class sourceClass, String root, String... exemptions) throws IOException {
        final String pairs[][] = new String[][]{
                // these happen in order, so we want to turn our longs to ints first, then do char to long, we can't actually discriminate between "Long" as text and "Long" as a type,
                // so we are going to fail for integers here, but it is hopefully enough to use just for the Timsort kernel that needs it.
                {"Long","Int"},
                {"long","int"},
                {"LONG","INT"},
                {"Character","Integer"},
                {"Char","Int"},
                {"char","int"},
                {"character","int"},
                {"CHAR","INT"}
        };
        return replaceAll(sourceClass, null, exemptions, root, pairs);
    }

    public static String intToObject(Class sourceClass, String root, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, null, exemptions, "Object", "Object", "Object", "Object", root, "OBJECT");
    }

    private static String intToObject(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions, "Object", "Object", "Object", "Object", root, "OBJECT");
    }

    private static String intToChar(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions, "Character", "Char", "char", "char", root, "CHAR");
    }

    private static String intToByte(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions, "Byte", "Byte", "byte", "byte", root, "BYTE");
    }

    private static String intToDouble(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions, "Double", "Double", "double", "double", root, "DOUBLE");
    }

    private static String intToFloat(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions,"Float","Float","float","float", root, "FLOAT");
    }

    private static String intToLong(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions,"Long","Long","long","long", root, "LONG");
    }

    private static String intToShort(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        return replicateCodeBasedOnInt(sourceClass, serialVersionUIDs, exemptions, "Short", "Short", "short", "short", root, "SHORT");
    }

    private static void shortToByte(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClass, serialVersionUIDs, exemptions, "Byte", "byte", root, "BYTE");
    }

    private static void shortToDouble(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClass, serialVersionUIDs, exemptions, "Double", "double", root, "DOUBLE");
    }

    private static void shortToFloat(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClass, serialVersionUIDs, exemptions,"Float","float", root, "FLOAT");
    }

    private static void shortToInteger(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClass, serialVersionUIDs, exemptions,"Integer","int", root, "INT", new String[][]{{"DbShortArray", "DbIntArray"}});
    }

    private static void shortToLong(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        replicateCodeBasedOnShort(sourceClass, serialVersionUIDs, exemptions, "Long", "long", root, "LONG", new String[][]{{"Integer.signum", "Long.signum"}});
    }

    private static void floatToDouble(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        replicateCodeBasedOnFloat(sourceClass, serialVersionUIDs, exemptions, "Double", "double", root, "DOUBLE");
    }

    public static List<String> charToAll(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToAll(sourceClass, root, null, exemptions);
    }

    private static List<String> charToAll(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(charToBoolean(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(charToByte(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(charToDouble(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(charToFloat(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(charToInteger(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(charToLong(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(charToShort(sourceClass, root, serialVersionUIDs, exemptions));
        return results;
    }

    public static List<String> charToIntegers(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToIntegers(sourceClass, root, null, exemptions);
    }

    public static List<String> charToAllButBoolean(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToAllButBoolean(sourceClass, root, null, exemptions);
    }

    public static String charToByte(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToByte(sourceClass, root, null, exemptions);
    }

    public static String charToObject(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToObject(sourceClass, root, null, exemptions);
    }

    public static String charToBoolean(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToBoolean(sourceClass, root, null, exemptions);
    }

    public static String charToLong(Class sourceClass, String root, String... exemptions) throws IOException {
        return charToLong(sourceClass, root, null, exemptions);
    }

    public static void charToAllButBooleanAndLong(Class sourceClass, String root, String... exemptions) throws IOException {
        charToAllButBooleanAndLong(sourceClass, root, null, exemptions);
    }

    public static List<String> charToAllButBoolean(Class sourceClass, String root, Map<String,Long> serialVersionUIDs, String... exemptions) throws IOException {
        final List<String> resultFiles = new ArrayList<>();
        resultFiles.add(charToByte(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToDouble(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToFloat(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToInteger(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToLong(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToShort(sourceClass, root, serialVersionUIDs, exemptions));

        return resultFiles;
    }

    public static List<String> charToIntegers(Class sourceClass, String root, Map<String,Long> serialVersionUIDs, String... exemptions) throws IOException {
        final List<String> resultFiles = new ArrayList<>();
        resultFiles.add(charToByte(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToShort(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToInteger(sourceClass, root, serialVersionUIDs, exemptions));
        resultFiles.add(charToLong(sourceClass, root, serialVersionUIDs, exemptions));
        return resultFiles;
    }

    private static void charToAllButBooleanAndLong(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        charToByte(sourceClass, root, serialVersionUIDs, exemptions);
        charToDouble(sourceClass, root, serialVersionUIDs, exemptions);
        charToFloat(sourceClass, root, serialVersionUIDs, exemptions);
        charToInteger(sourceClass, root, serialVersionUIDs, exemptions);
        charToShort(sourceClass, root, serialVersionUIDs, exemptions);
    }

    public static void charToAllButBooleanAndByte(Class sourceClass, String root, String... exemptions) throws IOException {
        charToAllButBooleanAndByte(sourceClass, root, null, exemptions);
    }

    private static void charToAllButBooleanAndByte(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        charToDouble(sourceClass, root, serialVersionUIDs, exemptions);
        charToFloat(sourceClass, root, serialVersionUIDs, exemptions);
        charToInteger(sourceClass, root, serialVersionUIDs, exemptions);
        charToLong(sourceClass, root, serialVersionUIDs, exemptions);
        charToShort(sourceClass, root, serialVersionUIDs, exemptions);
    }

    public static void charToAllButBooleanAndFloats(Class sourceClass, String root, String... exemptions) throws IOException {
        charToAllButBooleanAndFloats(sourceClass, root, null, exemptions);
    }

    public static void charToShortAndByte(Class sourceClass, String root, String... exemptions) throws IOException {
        charToByte(sourceClass, root, null, exemptions);
        charToShort(sourceClass, root, null, exemptions);
    }

    public static void charToAllButBooleanAndFloats(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        charToInteger(sourceClass, root, serialVersionUIDs, exemptions);
        charToByte(sourceClass, root, serialVersionUIDs, exemptions);
        charToLong(sourceClass, root, serialVersionUIDs, exemptions);
        charToShort(sourceClass, root, serialVersionUIDs, exemptions);
    }


    public static void shortToAllNumericals(Class sourceClass, String root, Map<String,Long> serialVersionUIDs, String... exemptions) throws IOException {
        shortToByte(sourceClass, root, serialVersionUIDs, exemptions);
        shortToDouble(sourceClass, root, serialVersionUIDs, exemptions);
        shortToFloat(sourceClass, root, serialVersionUIDs, exemptions);
        shortToInteger(sourceClass, root, serialVersionUIDs, exemptions);
        shortToLong(sourceClass, root, serialVersionUIDs, exemptions);
    }

    public static void intToAllNumericals(Class sourceClass, String root, Map<String,Long> serialVersionUIDs, String... exemptions) throws IOException {
        intToByte(sourceClass, root, serialVersionUIDs, exemptions);
        intToDouble(sourceClass, root, serialVersionUIDs, exemptions);
        intToFloat(sourceClass, root, serialVersionUIDs, exemptions);
        intToLong(sourceClass, root, serialVersionUIDs, exemptions);
        intToShort(sourceClass, root, serialVersionUIDs, exemptions);
    }

    public static List<String> intToAllButBoolean(Class sourceClass, String root, String... exemptions) throws IOException {
        return intToAllButBoolean(sourceClass, root, null, exemptions);
    }

    public static List<String> intToAllButBoolean(Class sourceClass, String root, Map<String,Long> serialVersionUIDs, String... exemptions) throws IOException {
        final List<String> results = new ArrayList<>();
        results.add(intToChar(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(intToByte(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(intToLong(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(intToShort(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(intToDouble(sourceClass, root, serialVersionUIDs, exemptions));
        results.add(intToFloat(sourceClass, root, serialVersionUIDs, exemptions));
        return results;
    }

    public static void shortToAllIntegralTypes(Class sourceClass, String root, String... exemptions) throws IOException {
        shortToAllIntegralTypes(sourceClass, root, null, exemptions);
    }

    private static void shortToAllIntegralTypes(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
            shortToByte(sourceClass, root, serialVersionUIDs, exemptions);
            shortToInteger(sourceClass, root, serialVersionUIDs, exemptions);
            shortToLong(sourceClass, root, serialVersionUIDs, exemptions);
        }

    public static void floatToAllFloatingPoints(Class sourceClass, String root, String... exemptions) throws IOException {
        floatToAllFloatingPoints(sourceClass, root, null, exemptions);
    }

    private static void floatToAllFloatingPoints(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        floatToDouble(sourceClass, root, serialVersionUIDs, exemptions);
    }

    public static String replaceAll(Class sourceClass, Map<String, Long> serialVersionUIDs, String exemptions[], String root, String[]... pairs) throws IOException {
        final String basePath = basePathForClass(sourceClass, root);
        InputStream inputStream = new FileInputStream(basePath + "/" + sourceClass.getSimpleName() + ".java");
        int nextChar;
        final StringBuilder inputText = new StringBuilder();
        while ((nextChar = inputStream.read()) != -1) {
            inputText.append((char) nextChar);
        }
        inputStream.close();

        final String className = replaceAll(sourceClass.getSimpleName(), null, exemptions, pairs);
        final String fullClassName = sourceClass.getPackage().getName() + "." + className;
        Long serialVersionUID = serialVersionUIDs == null ? null : serialVersionUIDs.get(fullClassName);

        String fullPath = basePath +  "/" + className + ".java";
        System.out.println("Generating java file " + fullPath);
        PrintWriter out = new PrintWriter(fullPath);
        out.println("/* ---------------------------------------------------------------------------------------------------------------------");
        out.println(" * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit " + sourceClass.getSimpleName() + " and regenerate");
        out.println(" * ------------------------------------------------------------------------------------------------------------------ */");
        out.print(replaceAll(inputText.toString(), serialVersionUID,exemptions, pairs));
        out.flush();
        out.close();

        return fullPath;
    }

    @NotNull
    public static String basePathForClass(Class sourceClass, String root) {
        return getModuleName(sourceClass) + "/"+ root +"/" + sourceClass.getPackage().getName().replace('.', '/');
    }

    @NotNull
    public static String pathForClass(Class sourceClass, String root) {
        return basePathForClass(sourceClass, root) +  "/" + sourceClass.getSimpleName() + ".java";
    }

    public static String replaceAll(String inputText, Long  serialVersionUID, String exemptions[],String[]... pairs) {
        String result = inputText;
        for (int i = 0; i < exemptions.length; i++) {
            String exemption = exemptions[i];
            result =  result.replaceAll(exemption,"@_@_exemption" + i + "_@_@");
        }
        for (int i = 0; i < pairs.length; i++) {
            String[] pair = pairs[i];
            result =  result.replaceAll(pair[0],"@_@_pair" + i + "_@_@");
        }
        for (int i = 0; i < pairs.length; i++) {
            String[] pair = pairs[i];
            result =  result.replaceAll("@_@_pair" + i + "_@_@",pair[1]);
        }
        for (int i = 0; i < exemptions.length; i++) {
            String exemption = exemptions[i];
            result =  result.replaceAll("@_@_exemption" + i + "_@_@",exemption);
        }

        if (serialVersionUID != null) {
            result = result.replaceAll("(\\s+(private\\s+)?(static\\s+)?(final\\s+)?long\\s+serialVersionUID\\s+=\\s+)\\-?[0-9]+L\\s*;", "$1" + serialVersionUID + "L;");
        }

        return result;
    }

    private static String getModuleName(Class sourceClass) {
        for (File file: new File(".").listFiles()) {
            // there is a folder 'lib' that exists during the build process that matches the the third startsWith
            // If we are in this package, don't bother looking at the other two.
            if(sourceClass.getName().startsWith("io.deephaven.libs.primitives")) {
                if (file.isDirectory() && file.getName().equals("DB")) {
                    return file.getPath();
                }
            } else {
                if (file.isDirectory() && sourceClass.getName().startsWith("io.deephaven." + file.getName().toLowerCase().replace('-', '_') + ".")) {
                    return file.getPath();
                }
            }
        }
        throw new RuntimeException("Unable to find " + sourceClass);
    }

    public static void intToLongAndFloatingPoints(Class sourceClass, String root, Map<String, Long> serialVersionUIDs, String... exemptions) throws IOException {
        intToDouble(sourceClass, root, serialVersionUIDs, exemptions);
        intToFloat(sourceClass, root, serialVersionUIDs, exemptions);
        intToLong(sourceClass, root, serialVersionUIDs, exemptions);
    }

    public static void intToLongAndFloatingPoints(Class sourceClass, String root, String... exemptions) throws IOException {
        intToLongAndFloatingPoints(sourceClass, root, null, exemptions);
    }
}
