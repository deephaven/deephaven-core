//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GenerateArrowColumnSourceTests {

    public static void main(String[] args) {
        List<Long> expectedRows = Arrays.asList(0L, 1L, 2L, 4L, 8L, 9L);

        generateTests("ArrowIntVectorTest", "/int_vector.arrow", "getInt", "WritableIntChunk",
                int.class, "NULL_INT", expectedRows,
                Arrays.asList(76428765, 9480636, null, 164366688, -1575247918, -23126));

        generateTests("ArrowBigIntVectorTest", "/big_int_vector.arrow", "getLong", "WritableLongChunk",
                long.class, "NULL_LONG", expectedRows,
                Arrays.asList(948377488, 2136, null, 999, -1327, 9821));

        generateTests("ArrowSmallIntVectorTest", "/small_int_vector.arrow", "getShort", "WritableShortChunk",
                short.class, "NULL_SHORT", expectedRows,
                Arrays.asList(-7812, 1, null, 13621, 0, 9752));

        generateTests("ArrowTinyVectorTest", "/tiny_int_vector.arrow", "getByte", "WritableByteChunk",
                byte.class, "NULL_BYTE", expectedRows,
                Arrays.asList(1, 37, null, -1, 127, -126));

        generateTests("ArrowUint2VectorTest", "/uint2_vector.arrow", "getChar", "WritableCharChunk",
                char.class, "NULL_CHAR", expectedRows,
                Arrays.asList("'a'", "'b'", null, "'c'", "'d'", "'e'"));

        generateTests("ArrowFloat4VectorTest", "/float4_vector.arrow", "getFloat", "WritableFloatChunk",
                float.class, "NULL_FLOAT", expectedRows,
                Arrays.asList(78.786F, 2.9F, null, 13.2F, 0, 21.01F));

        generateTests("ArrowFloat8VectorTest", "/float8_vector.arrow", "getDouble", "WritableDoubleChunk",
                double.class, "NULL_DOUBLE", expectedRows,
                Arrays.asList(-78.786, 2.9, null, 13.2, 0, -621.01));

        generateTests("ArrowBitVectorTest", "/bit_vector.arrow", "getBoolean", "WritableObjectChunk",
                Boolean.class, null, expectedRows,
                Arrays.asList(true, false, null, true, false, true));

        generateTests("ArrowUint1VectorTest", "/uint1_vector.arrow", "getShort", "WritableShortChunk",
                short.class, "NULL_SHORT", expectedRows,
                Arrays.asList(1, 21, null, 32, 0, 96));

        generateTests("ArrowUint4VectorTest", "/uint4_vector.arrow", "getLong", "WritableLongChunk",
                long.class, "NULL_LONG", expectedRows,
                Arrays.asList(453, 192, null, 0, 213, 96));

        generateTests("ArrowUint8VectorTest", "/uint8_vector.arrow", "get", "WritableObjectChunk",
                BigInteger.class, null, expectedRows,
                addWrapper("java.math.BigInteger.valueOf(%s)", Arrays.asList(
                        "4533434535435L", "192547560007L", null, "0", "213", "1")));

        generateTests("ArrowVarCharVectorTest", "/var_char_vector.arrow", "get", "WritableObjectChunk",
                String.class, null, expectedRows,
                Arrays.asList("\"my\"", "\"vector\"", null, "\"it\"", "\"is\"", "\"success\""));

        generateTests("ArrowVarBinaryVectorTest", "/var_binary_vector.arrow", "get", "WritableObjectChunk",
                byte[].class, null, expectedRows,
                addWrapper("%s.getBytes()", Arrays.asList(
                        "\"hi\"", "\"how are you?\"", null, "\"I'm fine\"", "\"It is great\"", "\"See you soon\"")));

        generateTests("ArrowFixedSizeBinaryVectorTest", "/fixed_size_binary_vector.arrow", "get", "WritableObjectChunk",
                byte[].class, null, expectedRows,
                addWrapper("%s.getBytes()", Arrays.asList("\"hi\"", "\"it\"", null, "\"is\"", "\"me\"", "\"!!\"")));

        generateTests("ArrowDateMilliVectorTest", "/date_milli_vector.arrow", "get", "WritableObjectChunk",
                LocalDateTime.class, null, expectedRows,
                addWrapper("java.time.LocalDateTime.of(%s)", Arrays.asList(
                        "2022, 11, 30, 23, 0, 0",
                        "2000, 1, 16, 23, 0, 0",
                        null,
                        "2022, 1, 31, 22, 8, 2",
                        "1920, 1, 31, 23, 8, 2",
                        "1900, 1, 31, 23, 22, 46")));

        generateTests("ArrowTimeMilliVectorTest", "/time_milli_vector.arrow", "get", "WritableObjectChunk",
                LocalTime.class, null, expectedRows,
                addWrapper("java.time.LocalTime.of(%s)", Arrays.asList(
                        "0, 0, 0, 864000000",
                        "0, 0, 0, 908000000",
                        null,
                        "0, 0, 0, 0",
                        "0, 14, 34, 300000000",
                        "21, 59, 29, 26000000")));

        generateTests("ArrowTimestampVectorTest", "/timestamp_vector.arrow", "get", "WritableObjectChunk",
                null, ClassName.get("java.time", "Instant"), null, expectedRows,
                addWrapper("io.deephaven.time.DateTimeUtils.epochNanosToInstant(%s)", Arrays.asList(
                        "1670443801", "1570443532", null, "0", "170443801", "-72309740")));

        generateTests("ArrowDecimalVectorTest", "/decimal_vector.arrow", "get", "WritableObjectChunk",
                BigDecimal.class, null, expectedRows,
                addWrapper("new java.math.BigDecimal(\"%s\", java.math.MathContext.DECIMAL64)\n", Arrays.asList(
                        "1000878769709809808089098089088.533",
                        "1021321435356657878768767886762.533",
                        null,
                        "1021321435311117878768767886762.112",
                        "7032447742342342342342342342344.145",
                        "6712398032923494320982348023490.555")));

        generateTests("ArrowDecimal256VectorTest", "/decimal256_vector.arrow", "get", "WritableObjectChunk",
                BigDecimal.class, null, expectedRows,
                addWrapper("new java.math.BigDecimal(\"%s\", java.math.MathContext.DECIMAL64)\n", Arrays.asList(
                        "1000878769709809808089098089088.533",
                        "1021321435356657878768767886762.533",
                        null,
                        "1021321435311117878768767886762.112",
                        "7032447742342342342342342342344.145",
                        "6712398032923494320982348023490.555")));
    }

    private static List<String> addWrapper(String wrapper, List<String> values) {
        return values.stream().map(value -> value == null
                ? null
                : String.format(wrapper, value)).collect(Collectors.toList());
    }

    private static AnnotationSpec junitRule() {
        return AnnotationSpec.builder(ClassName.get("org.junit", "Rule")).build();
    }

    private static AnnotationSpec junitTest() {
        return AnnotationSpec.builder(ClassName.get("org.junit", "Test")).build();
    }

    private static TypeName engineCleanup() {
        return ClassName.get("io.deephaven.engine.testutil.junit4", "EngineCleanup");
    }

    private static TypeName queryTable() {
        return ClassName.get("io.deephaven.engine.table.impl", "QueryTable");
    }

    private static ClassName arrowWrapperTools() {
        return ClassName.get("io.deephaven.extensions.arrow", "ArrowWrapperTools");
    }

    private static TypeName arrowWrapperToolsShareable() {
        return arrowWrapperTools().nestedClass("Shareable");
    }

    private static TypeName list(final TypeName valueType) {
        return ParameterizedTypeName.get(ClassName.get(List.class), valueType);
    }

    private static TypeName columnSource(final TypeName valueType) {
        return ParameterizedTypeName.get(ClassName.get("io.deephaven.engine.table", "ColumnSource"),
                valueType.isPrimitive() ? valueType.box() : valueType);
    }

    private static TypeName mutableInt() {
        return ClassName.get("io.deephaven.util.mutable", "MutableInt");
    }

    private static TypeName staticAssert() {
        return ClassName.get("org.junit", "Assert");
    }

    private static TypeName fillContext() {
        return ClassName.get("io.deephaven.engine.table", "ChunkSource").nestedClass("FillContext");
    }

    public static void generateTests(
            String className,
            String fileName,
            String getMethodName,
            String chunkClass,
            Class<?> valueType,
            String nullValue,
            List<Long> expectedRows,
            List<?> expectedValues) {
        generateTests(className, fileName, getMethodName, chunkClass, valueType, TypeName.get(valueType), nullValue,
                expectedRows, expectedValues);
    }

    public static void generateTests(
            String className,
            String fileName,
            String getMethodName,
            String chunkClass,
            Class<?> valueTypeCls,
            TypeName valueType,
            String nullValue,
            List<Long> expectedRows,
            List<?> expectedValues) {

        final TypeName testClassType = ClassName.get("io.deephaven.extensions.arrow", className);

        final TypeSpec test = TypeSpec
                .classBuilder(className)
                .addModifiers(Modifier.PUBLIC)
                .addField(getEngineCleanupRule())
                .addField(getExpectedRows(expectedRows))
                .addField(getExpectedValues(valueTypeCls, valueType, nullValue, expectedValues))
                .addMethod(generateLoadTable(testClassType, fileName))
                .addMethod(generateTestReadArrowFile(getMethodName, valueTypeCls, valueType))
                .addMethod(generateTestFillChunk(chunkClass, valueTypeCls, valueType))
                .build();

        JavaFile javaFile = JavaFile.builder("io.deephaven.extensions.arrow", test)
                .indent("    ")

                .skipJavaLangImports(true)
                .build();

        String path = "extensions/arrow/src/test/java/io/deephaven/extensions/arrow/" + className + ".java";
        try {
            String gradleTask = "generateArrowColumnTestSources";
            Class<?> generatorClass = GenerateArrowColumnSourceTests.class;
            final String header = String.join("\n",
                    "//",
                    "// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending",
                    "//",
                    "// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY",
                    "// ****** Run " + generatorClass.getSimpleName() + " or \"./gradlew " + gradleTask
                            + "\" to regenerate",
                    "//",
                    "// @formatter:off",
                    "");

            Files.write(Paths.get(path), (header + javaFile).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static FieldSpec getEngineCleanupRule() {
        return FieldSpec.builder(engineCleanup(), "framework", Modifier.PUBLIC, Modifier.FINAL)
                .addAnnotation(junitRule())
                .initializer("new $T()", engineCleanup())
                .build();
    }

    private static FieldSpec getExpectedRows(final List<Long> expectedRows) {
        CodeBlock.Builder initializer = CodeBlock.builder()
                .add("$T.asList(", Arrays.class);
        boolean first = true;
        for (Long row : expectedRows) {
            if (first) {
                first = false;
                initializer.add("$LL", row);
            } else {
                initializer.add(", $LL", row);
            }
        }
        initializer.add(")");
        return FieldSpec.builder(list(ClassName.get(Long.class)), "expectedRows",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(initializer.build())
                .build();
    }

    private static FieldSpec getExpectedValues(
            final Class<?> valueTypeCls, final TypeName valueType, final String nullValue,
            final List<?> expectedValues) {
        final String literalSuffix;
        if (valueTypeCls == long.class) {
            literalSuffix = "L";
        } else if (valueTypeCls == float.class) {
            literalSuffix = "F";
        } else if (valueTypeCls == double.class) {
            literalSuffix = "D";
        } else {
            literalSuffix = "";
        }

        CodeBlock.Builder initializer = CodeBlock.builder()
                .add("new $T[] {", valueType);
        boolean first = true;
        for (Object row : expectedValues) {
            if (first) {
                first = false;
            } else {
                initializer.add(", ");
            }

            if (row == null && nullValue != null) {
                initializer.add("$T.$L", ClassName.get("io.deephaven.util", "QueryConstants"), nullValue);
            } else {
                initializer.add("$L" + literalSuffix, row);
            }
        }
        initializer.add("}");
        return FieldSpec.builder(ArrayTypeName.of(valueType), "expectedValues",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(initializer.build())
                .build();
    }

    private static MethodSpec generateLoadTable(final TypeName testClass, final String fileName) {
        return MethodSpec.methodBuilder("loadTable")
                .returns(queryTable())
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .addStatement("//noinspection ConstantConditions")
                .addStatement("final $T dataFile = new $T($T.class.getResource($S).getFile())",
                        File.class, File.class, testClass, fileName)
                .addStatement("return $T.readFeather(dataFile.getPath())", arrowWrapperTools())
                .build();
    }

    private static String createComparator(final Class<?> valueType, final String expected, final String actual) {
        String extraArg = "";
        if (valueType == float.class || valueType == double.class) {
            extraArg = ", 1e-5";
        }
        String arrayMethod = "";
        if (valueType != null && valueType.isArray()) {
            arrayMethod = "Array";
        }
        return "$T.assert" + arrayMethod + "Equals(" + expected + ", " + actual + extraArg + ")";
    }

    private static MethodSpec generateTestReadArrowFile(
            final String getMethodName, final Class<?> valueTypeCls, final TypeName valueType) {
        return MethodSpec.methodBuilder("testReadArrowFile")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(junitTest())
                .addStatement("final $T table = loadTable()", queryTable())
                .addStatement("$T.assertEquals(expectedValues.length, table.intSize())", staticAssert())
                .addCode("\n")
                .addStatement("// check that the expected rows are present")
                .addStatement("final $T actualRows = new $T<>()", list(ClassName.get(Long.class)), ArrayList.class)
                .addStatement("table.getRowSet().forAllRowKeys(actualRows::add)")
                .addStatement("$T.assertEquals(expectedRows, actualRows)", staticAssert())
                .addCode("\n")
                .addStatement("$T.assertEquals(1, table.getColumnSources().size())", staticAssert())
                .addStatement("// noinspection OptionalGetWithoutIsPresent, unchecked")
                .addStatement("final $T cs = ($T)table.getColumnSources().stream().findFirst().get()",
                        columnSource(valueType), columnSource(valueType))
                .addCode("\n")
                .addStatement("$T.resetNumBlocksLoaded()", arrowWrapperToolsShareable())
                .addStatement("final $T pos = new $T()", mutableInt(), mutableInt())
                .addCode("table.getRowSet().forAllRowKeys(rowKey -> ")
                .addCode(createComparator(valueTypeCls, "expectedValues[pos.getAndIncrement()]", "cs.$L(rowKey)"),
                        staticAssert(), getMethodName)
                .addStatement(")")
                .addStatement("Assert.assertEquals(3, $T.numBlocksLoaded())", arrowWrapperToolsShareable())
                .build();
    }

    private static MethodSpec generateTestFillChunk(
            final String chunkTypeName, final Class<?> valueTypeCls, final TypeName valueType) {
        final TypeName attributeValues = ClassName.get("io.deephaven.chunk.attributes", "Values");
        final ClassName chunkTypeCls = ClassName.get("io.deephaven.chunk", chunkTypeName);
        final TypeName chunkType;
        if (valueType.isPrimitive()) {
            chunkType = ParameterizedTypeName.get(chunkTypeCls, attributeValues);
        } else {
            chunkType = ParameterizedTypeName.get(chunkTypeCls, valueType, attributeValues);
        }

        return MethodSpec.methodBuilder("testFillChunk")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(junitTest())
                .addStatement("final $T table = loadTable()", queryTable())
                .addCode("\n")
                .addStatement("// noinspection OptionalGetWithoutIsPresent, unchecked")
                .addStatement("final $T cs = ($T)table.getColumnSources().stream().findFirst().get()",
                        columnSource(valueType), columnSource(valueType))
                .addCode("\n")
                .addStatement("try (final $T fillContext = cs.makeFillContext(table.intSize())$>", fillContext())
                .beginControlFlow("final $T chunk = $T.makeWritableChunk(table.intSize()))$<", chunkType, chunkTypeCls)
                .addCode("\n")
                .addStatement("$T.resetNumBlocksLoaded()", arrowWrapperToolsShareable())
                .addStatement("cs.fillChunk(fillContext, chunk, table.getRowSet())")
                .addStatement("Assert.assertEquals(3, $T.numBlocksLoaded())", arrowWrapperToolsShareable())
                .addCode("\n")
                .beginControlFlow("for (int ii = 0; ii < expectedValues.length; ++ii)")
                .addStatement(createComparator(valueTypeCls, "expectedValues[ii]", "chunk.get(ii)"), staticAssert())
                .endControlFlow()
                .endControlFlow()
                .build();
    }
}
