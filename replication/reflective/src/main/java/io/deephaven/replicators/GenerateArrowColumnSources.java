//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Modifier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GenerateArrowColumnSources {

    public static final String ARROW_VECTOR_PREFIX = "Arrow Vector: {@link ";
    public static final String RETURNS_PREFIX = "Deephaven Type: ";
    private static final String JAVADOC_TEMPLATE = ARROW_VECTOR_PREFIX + "%s}\n" + RETURNS_PREFIX + "%s";

    public static void main(String[] args) {
        generateArrowColumnSource("ArrowIntColumnSource", Integer.class, int.class, IntVector.class,
                "getInt", "ForInt", WritableIntChunk.class, List.of(
                        generatePrivateExtractMethod(int.class, IntVector.class, "NULL_INT")));

        generateArrowColumnSource("ArrowLongColumnSource", Long.class, long.class, BigIntVector.class,
                "getLong", "ForLong", WritableLongChunk.class, List.of(
                        generatePrivateExtractMethod(long.class, BigIntVector.class, "NULL_LONG")));

        generateArrowColumnSource("ArrowDoubleColumnSource", Double.class, double.class, Float8Vector.class,
                "getDouble", "ForDouble", WritableDoubleChunk.class, List.of(
                        generatePrivateExtractMethod(double.class, Float8Vector.class, "NULL_DOUBLE")));

        generateArrowColumnSource("ArrowFloatColumnSource", Float.class, float.class, Float4Vector.class,
                "getFloat", "ForFloat", WritableFloatChunk.class, List.of(
                        generatePrivateExtractMethod(float.class, Float4Vector.class, "NULL_FLOAT")));

        generateArrowColumnSource("ArrowCharColumnSource", Character.class, char.class, UInt2Vector.class,
                "getChar", "ForChar", WritableCharChunk.class, List.of(
                        generatePrivateExtractMethod(char.class, UInt2Vector.class, "NULL_CHAR")));

        generateArrowColumnSource("ArrowShortColumnSource", Short.class, short.class, SmallIntVector.class,
                "getShort", "ForShort", WritableShortChunk.class, List.of(
                        generatePrivateExtractMethod(short.class, SmallIntVector.class, "NULL_SHORT")));

        generateArrowColumnSource("ArrowByteColumnSource", Byte.class, byte.class, TinyIntVector.class,
                "getByte", "ForByte", WritableByteChunk.class, List.of(
                        generatePrivateExtractMethod(byte.class, TinyIntVector.class, "NULL_BYTE")));

        generateArrowColumnSource("ArrowBooleanColumnSource", Boolean.class, boolean.class, BitVector.class,
                "get", "ForBoolean", WritableObjectChunk.class, List.of(
                        generatePrivateObjectExtractMethod(Boolean.class, BitVector.class)));

        generateArrowColumnSource("ArrowUInt1ColumnSource", Short.class, short.class, UInt1Vector.class,
                "getShort", "ForShort", WritableShortChunk.class, List.of(
                        generatePrivateExtractMethod(short.class, UInt1Vector.class, "NULL_SHORT", true)));

        generateArrowColumnSource("ArrowUInt4ColumnSource", Long.class, long.class, UInt4Vector.class,
                "getLong", "ForLong", WritableLongChunk.class, List.of(
                        generatePrivateExtractMethod(long.class, UInt4Vector.class, "NULL_LONG", true)));

        generateArrowColumnSource("ArrowUInt8ColumnSource", BigInteger.class, BigInteger.class, UInt8Vector.class,
                "get", "ForObject", WritableObjectChunk.class, List.of(
                        generatePrivateExtractMethod(BigInteger.class, UInt8Vector.class, null, true)));

        generateArrowColumnSource("ArrowStringColumnSource", String.class, String.class, VarCharVector.class,
                "get", "ForObject", WritableObjectChunk.class, List.of(
                        preparePrivateExtractMethod(String.class, VarCharVector.class)
                                .addStatement(
                                        "return vector.isSet(posInBlock) == 0 ? null : new $T(vector.get(posInBlock))",
                                        String.class)
                                .build()));

        generateArrowColumnSource("ArrowObjectColumnSource", (Class<?>) null, null, ValueVector.class,
                "get", "ForObject", WritableObjectChunk.class, List.of(
                        generatePrivateObjectExtractMethod(null, ValueVector.class)));

        generateArrowColumnSource("ArrowLocalTimeColumnSource", LocalTime.class, LocalTime.class, TimeMilliVector.class,
                "get", "ForObject", WritableObjectChunk.class, List.of(
                        preparePrivateExtractMethod(LocalTime.class, TimeMilliVector.class)
                                .addStatement("$T localDateTime = vector.getObject(posInBlock)", LocalDateTime.class)
                                .addStatement("return localDateTime != null ? localDateTime.toLocalTime() : null")
                                .build()));

        final ClassName instant = ClassName.get("java.time", "Instant");
        generateArrowColumnSource("ArrowInstantColumnSource", instant, instant, TimeStampVector.class,
                "get", "ForObject", WritableObjectChunk.class, List.of(
                        preparePrivateExtractMethod(instant, TimeStampVector.class)
                                .addStatement(
                                        "return vector.isSet(posInBlock) == 0 ? null : io.deephaven.time.DateTimeUtils.epochNanosToInstant(vector.get(posInBlock))",
                                        instant)
                                .build()));
    }

    @NotNull
    private static MethodSpec.Builder preparePrivateExtractMethod(
            final Class<?> returnsCls, final Class<?> vectorClass) {
        final TypeName returns = returnsCls != null ? TypeName.get(returnsCls) : TypeVariableName.get("T");
        return preparePrivateExtractMethod(returns, vectorClass);
    }

    @NotNull
    private static MethodSpec.Builder preparePrivateExtractMethod(
            final TypeName returns, final Class<?> vectorClass) {
        return MethodSpec
                .methodBuilder("extract")
                .addModifiers(Modifier.PRIVATE)
                .returns(returns)
                .addParameter(TypeName.INT, "posInBlock", Modifier.FINAL)
                .addParameter(TypeName.get(vectorClass), "vector", Modifier.FINAL);
    }

    @NotNull
    private static MethodSpec generatePrivateExtractMethod(
            final Class<?> returns, final Class<?> vectorClass, final String nullPrimitive) {
        return generatePrivateExtractMethod(returns, vectorClass, nullPrimitive, false);
    }

    @NotNull
    private static MethodSpec generatePrivateExtractMethod(
            final Class<?> returns, final Class<?> vectorClass, final String nullPrimitive,
            final boolean getNoOverflow) {
        MethodSpec.Builder builder = preparePrivateExtractMethod(returns, vectorClass);

        final String vectorGet = getNoOverflow
                ? vectorClass.getSimpleName() + ".getNoOverflow(vector.getDataBuffer(), posInBlock)"
                : "vector.get(posInBlock)";

        if (nullPrimitive == null) {
            builder.addStatement("return vector.isSet(posInBlock) == 0 ? null : " + vectorGet);
        } else {
            builder.addStatement("return vector.isSet(posInBlock) == 0 ? $T.$L : " + vectorGet,
                    queryConstants(), nullPrimitive);
        }

        return builder.build();
    }

    @NotNull
    private static MethodSpec generatePrivateObjectExtractMethod(
            final Class<?> returns, final Class<?> vectorClass) {
        return preparePrivateExtractMethod(returns, vectorClass)
                .addStatement("return " + (returns == null ? "(T)" : "") + "vector.getObject(posInBlock)")
                .build();
    }

    @NotNull
    private static MethodSpec generatePublicGetMethod(final String name, final TypeName returns) {
        return MethodSpec
                .methodBuilder(name)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addAnnotation(Override.class)
                .returns(returns)
                .addParameter(TypeName.LONG, "rowKey", Modifier.FINAL)
                .beginControlFlow("try ($T fc = ($T) makeFillContext(0))", fillContext(), fillContext())
                .addStatement("fc.ensureLoadingBlock(getBlockNo(rowKey))")
                .addStatement("return extract(getPositionInBlock(rowKey), fc.getVector(field))")
                .endControlFlow()
                .build();
    }

    private static ClassName queryConstants() {
        return ClassName.get("io.deephaven.util", "QueryConstants");
    }

    private static ClassName fillContext() {
        return ClassName.get("io.deephaven.extensions.arrow", "ArrowWrapperTools", "FillContext");
    }

    private static ClassName superFillContext() {
        return ClassName.get("io.deephaven.engine.table", "ChunkSource", "FillContext");
    }

    private static void generateArrowColumnSource(
            final String className,
            final Class<?> boxedTypeCls,
            final Class<?> typeCls,
            final Class<?> vectorType,
            final String getterName,
            final String getDefaultsInnerType,
            final Class<?> writableChunkType,
            final List<MethodSpec> methods) {
        final TypeName boxedType = boxedTypeCls != null ? TypeName.get(boxedTypeCls) : TypeVariableName.get("T");
        final TypeName type = typeCls != null ? TypeName.get(typeCls) : TypeName.get(Object.class);
        generateArrowColumnSource(className, boxedType, type, vectorType, getterName, getDefaultsInnerType,
                writableChunkType, methods);
    }

    private static void generateArrowColumnSource(
            final String className,
            final TypeName boxedType,
            final TypeName type,
            final Class<?> vectorType,
            final String getterName,
            final String getDefaultsInnerType,
            final Class<?> writableChunkType,
            final List<MethodSpec> methods) {
        final AnnotationSpec notNull = AnnotationSpec.builder(NotNull.class).build();

        final ClassName superType = ClassName.get("io.deephaven.extensions.arrow.sources", "AbstractArrowColumnSource");
        final ClassName defaultsImplCN = ClassName.get(
                "io.deephaven.engine.table.impl", "ImmutableColumnSourceGetDefaults", getDefaultsInnerType);
        final TypeName defaultsImpl;
        if (getDefaultsInnerType.contains("Object")) {
            defaultsImpl = ParameterizedTypeName.get(defaultsImplCN, boxedType);
        } else {
            defaultsImpl = defaultsImplCN;
        }
        final TypeSpec.Builder columnSourceBuilder = TypeSpec
                .classBuilder(className)
                .addJavadoc(String.format(JAVADOC_TEMPLATE, vectorType.getSimpleName(), type))
                .superclass(ParameterizedTypeName.get(superType, boxedType))
                .addSuperinterface(defaultsImpl)
                .addModifiers(Modifier.PUBLIC);

        final ClassName arrowTableContext = ClassName.get("io.deephaven.extensions.arrow", "ArrowWrapperTools",
                "ArrowTableContext");
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder();
        if (className.contains("Object")) {
            columnSourceBuilder.addTypeVariable(TypeVariableName.get("T"));
            constructor.addParameter(
                    ParameterizedTypeName.get(ClassName.get(Class.class), TypeVariableName.get("T")).annotated(notNull),
                    "type", Modifier.FINAL);
            constructor.addStatement("super(type, highBit, field, arrowTableContext)");
        } else {
            constructor.addStatement("super($T.class, highBit, field, arrowTableContext)", type);
        }
        constructor
                .addParameter(int.class, "highBit", Modifier.FINAL)
                .addParameter(TypeName.get(Field.class).annotated(notNull), "field", Modifier.FINAL)
                .addParameter(arrowTableContext.annotated(notNull), "arrowTableContext", Modifier.FINAL)
                .addModifiers(Modifier.PUBLIC);

        columnSourceBuilder.addMethod(constructor.build());

        final ClassName attributeValues = ClassName.get("io.deephaven.chunk.attributes", "Values");
        final TypeName writableChunk = ParameterizedTypeName.get(
                ClassName.get("io.deephaven.chunk", "WritableChunk"), WildcardTypeName.supertypeOf(attributeValues));
        final TypeName typedWritableChunk;
        if (writableChunkType.getSimpleName().contains("Object")) {
            typedWritableChunk = ParameterizedTypeName.get(ClassName.get(writableChunkType), boxedType,
                    WildcardTypeName.supertypeOf(attributeValues));
        } else {
            typedWritableChunk = ParameterizedTypeName.get(ClassName.get(writableChunkType),
                    WildcardTypeName.supertypeOf(attributeValues));
        }
        final ClassName rowSequence = ClassName.get("io.deephaven.engine.rowset", "RowSequence");
        columnSourceBuilder.addMethod(MethodSpec.methodBuilder("fillChunk")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addParameter(superFillContext().annotated(notNull), "context", Modifier.FINAL)
                .addParameter(writableChunk.annotated(notNull), "destination", Modifier.FINAL)
                .addParameter(rowSequence.annotated(notNull), "rowSequence", Modifier.FINAL)
                .addStatement("final $T chunk = destination.as$L()", typedWritableChunk,
                        writableChunkType.getSimpleName())
                .addStatement("final $T arrowContext = ($T) context", fillContext(), fillContext())
                .addStatement("chunk.setSize(0)")
                .addStatement(
                        "fillChunk(arrowContext, rowSequence, rowKey -> chunk.add(extract(getPositionInBlock(rowKey), arrowContext.getVector(field))))")
                .build());

        columnSourceBuilder.addMethod(generatePublicGetMethod(getterName, getterName.equals("get") ? boxedType : type));
        methods.forEach(columnSourceBuilder::addMethod);

        final TypeSpec columnSource = columnSourceBuilder.build();
        JavaFile javaFile = JavaFile.builder("io.deephaven.extensions.arrow.sources", columnSource)
                .indent("    ")
                .skipJavaLangImports(true)
                .build();

        String path = "extensions/arrow/src/main/java/io/deephaven/extensions/arrow/sources/" + className + ".java";
        try {
            String gradleTask = "generateArrowColumnSources";
            Class<?> generatorClass = GenerateArrowColumnSources.class;
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
}
