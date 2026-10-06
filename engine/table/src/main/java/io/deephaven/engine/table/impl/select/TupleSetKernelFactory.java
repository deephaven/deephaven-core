//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import com.google.common.io.BaseEncoding;
import com.palantir.javapoet.AnnotationSpec;
import com.palantir.javapoet.ArrayTypeName;
import com.palantir.javapoet.ClassName;
import com.palantir.javapoet.CodeBlock;
import com.palantir.javapoet.FieldSpec;
import com.palantir.javapoet.JavaFile;
import com.palantir.javapoet.MethodSpec;
import com.palantir.javapoet.ParameterSpec;
import com.palantir.javapoet.ParameterizedTypeName;
import com.palantir.javapoet.TypeName;
import com.palantir.javapoet.TypeSpec;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.tuple.ArrayTuple;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.lang.model.element.Modifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Generates the {@link TupleMapSetKernel} subclass for a compound key's column types. The subclasses for two columns
 * are generated ahead of time by {@code ReplicateTupleSetKernels}; those for more columns are generated and compiled
 * when first needed.
 * <p>
 * Two and three columns make the typed tuples of {@code io.deephaven.tuple.generated}, whose elements a probe is
 * compared with directly; more columns make an {@link ArrayTuple} of boxed values, which are unboxed to be compared.
 */
public class TupleSetKernelFactory {

    public static final String PACKAGE_NAME = "io.deephaven.engine.table.impl.select.tuplemap.gen";

    // Named rather than referenced, as the annotations are only on the compile classpath.
    private static final ClassName NOT_NULL = ClassName.get("org.jetbrains.annotations", "NotNull");
    private static final ClassName NULLABLE = ClassName.get("org.jetbrains.annotations", "Nullable");
    private static final String TUPLE_PACKAGE_NAME = "io.deephaven.tuple.generated";
    private static final String COMPARISONS_PACKAGE_NAME = "io.deephaven.util.compare";
    private static final String CLASS_PREFIX = "TupleSetKernel";
    private static final String[] ORDINALS = {"First", "Second", "Third"};

    private static final Map<String, Constructor<? extends TupleMapSetKernel>> CONSTRUCTORS =
            new ConcurrentHashMap<>();

    private TupleSetKernelFactory() {}

    /**
     * @param keySources The set table's key sources, reinterpreted to primitives; at least two
     * @return A kernel for {@code keySources}
     */
    static TupleMapSetKernel make(@NotNull final ColumnSource<?>[] keySources) {
        final ChunkType[] chunkTypes =
                Arrays.stream(keySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
        final String className = className(chunkTypes);
        final Constructor<? extends TupleMapSetKernel> constructor =
                CONSTRUCTORS.computeIfAbsent(className, ignored -> findOrCompile(chunkTypes, className));
        try {
            return constructor.newInstance((Object) keySources);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new UncheckedDeephavenException("Could not construct " + className, e);
        }
    }

    private static Constructor<? extends TupleMapSetKernel> findOrCompile(
            @NotNull final ChunkType[] chunkTypes,
            @NotNull final String className) {
        final Class<?> clazz;
        if (chunkTypes.length == 2) {
            try {
                clazz = Class.forName(PACKAGE_NAME + "." + className);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Missing pregenerated " + className, e);
            }
        } else {
            clazz = compile(chunkTypes, className);
        }
        try {
            return clazz.asSubclass(TupleMapSetKernel.class).getConstructor(ColumnSource[].class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Could not find constructor for " + className, e);
        }
    }

    private static Class<?> compile(@NotNull final ChunkType[] chunkTypes, @NotNull final String className) {
        final String javaString = Arrays.stream(generate(chunkTypes, className, false).toString().split("\n"))
                .filter(line -> !line.startsWith("package "))
                .collect(Collectors.joining("\n"));
        return ExecutionContext.getContext().getQueryCompiler().compile(QueryCompilerRequest.builder()
                .description("TupleSetKernelFactory: " + className)
                .className(className)
                .classBody(javaString)
                .packageNameRoot(PACKAGE_NAME)
                .build());
    }

    /**
     * @param chunkTypes The key's column chunk types, none of them {@link ChunkType#Boolean}
     * @return The name of the kernel class for {@code chunkTypes}
     */
    public static String className(@NotNull final ChunkType[] chunkTypes) {
        final String typeNames = Arrays.stream(chunkTypes).map(ChunkType::name).collect(Collectors.joining());
        if (chunkTypes.length <= 10) {
            return CLASS_PREFIX + typeNames;
        }
        // Keep long keys' class names short by hashing the type names.
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            return CLASS_PREFIX + "Hashed" + BaseEncoding.base16()
                    .encode(digest.digest(typeNames.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    /**
     * Generate the kernel class for {@code chunkTypes}.
     *
     * @param chunkTypes The key's column chunk types, at least two and none of them {@link ChunkType#Boolean}
     * @param className The class name, from {@link #className(ChunkType[])}
     * @param nullityAnnotations Whether to annotate the parameters of the overriding methods with {@code NotNull} and
     *        {@code Nullable}; the annotations are not on the classpath of a kernel compiled when needed
     * @return The generated source
     */
    public static JavaFile generate(
            @NotNull final ChunkType[] chunkTypes,
            @NotNull final String className,
            final boolean nullityAnnotations) {
        final ClassName notNull = nullityAnnotations ? NOT_NULL : null;
        final ClassName nullable = nullityAnnotations ? NULLABLE : null;
        final int columns = chunkTypes.length;
        final boolean typedTuple = columns <= 3;
        final ClassName tupleClass = typedTuple
                ? ClassName.get(TUPLE_PACKAGE_NAME,
                        Arrays.stream(chunkTypes).map(ChunkType::name).collect(Collectors.joining()) + "Tuple")
                : ClassName.get(ArrayTuple.class);
        final ClassName kernelClass = ClassName.get(PACKAGE_NAME, className);
        final ClassName probeClass = kernelClass.nestedClass("Probe");
        final ClassName strategyClass = kernelClass.nestedClass("Strategy");

        final TypeSpec.Builder probe = TypeSpec.classBuilder("Probe")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL);
        for (int ii = 0; ii < columns; ++ii) {
            probe.addField(elementType(chunkTypes[ii]), "k" + ii);
        }

        final MethodSpec.Builder hash = MethodSpec.methodBuilder("hash")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC).returns(int.class);
        for (int ii = 0; ii < columns; ++ii) {
            hash.addParameter(elementType(chunkTypes[ii]), "k" + ii);
        }
        hash.addStatement("int hash = $T.hashCode(k0)", comparisons(chunkTypes[0]));
        for (int ii = 1; ii < columns; ++ii) {
            hash.addStatement("hash = hash * 31 + $T.hashCode(k$L)", comparisons(chunkTypes[ii]), ii);
        }
        hash.addStatement("return hash");

        final String probeElements = IntStream.range(0, columns).mapToObj(ii -> "probe.k" + ii)
                .collect(Collectors.joining(", "));
        final MethodSpec strategyHashCode = MethodSpec.methodBuilder("hashCode")
                .addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(int.class)
                .addParameter(parameter(ClassName.get(Object.class), "key", notNull))
                .beginControlFlow("if (key instanceof $T)", probeClass)
                .addStatement("final $T probe = ($T) key", probeClass, probeClass)
                .addStatement("return hash(" + probeElements + ")")
                .endControlFlow()
                .addStatement("final $T tuple = ($T) key", tupleClass, tupleClass)
                .addStatement("return hash($L)", storedElements(chunkTypes, typedTuple, "tuple"))
                .build();

        final MethodSpec strategyEquals = MethodSpec.methodBuilder("equals")
                .addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(boolean.class)
                .addParameter(parameter(ClassName.get(Object.class), "lhs", nullable))
                .addParameter(parameter(ClassName.get(Object.class), "rhs", nullable))
                .beginControlFlow("if (lhs == null || rhs == null)")
                .addStatement("return lhs == rhs")
                .endControlFlow()
                .addComment("The map passes a stored tuple as rhs, and a probe or a tuple being added as lhs")
                .addStatement("final $T stored = ($T) rhs", tupleClass, tupleClass)
                .beginControlFlow("if (lhs instanceof $T)", probeClass)
                .addStatement("final $T probe = ($T) lhs", probeClass, probeClass)
                .addStatement("return $L", equalsExpression(chunkTypes, typedTuple,
                        ii -> CodeBlock.of("probe.k$L", ii)))
                .endControlFlow()
                .addStatement("final $T other = ($T) lhs", tupleClass, tupleClass)
                .addStatement("return $L", equalsExpression(chunkTypes, typedTuple,
                        ii -> storedElement(chunkTypes[ii], typedTuple, "other", ii)))
                .build();

        final TypeSpec strategy = TypeSpec.classBuilder("Strategy")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .addSuperinterface(ParameterizedTypeName.get(Hash.Strategy.class, Object.class))
                .addField(FieldSpec.builder(strategyClass, "INSTANCE",
                        Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                        .initializer("new $T()", strategyClass).build())
                .addMethod(strategyHashCode)
                .addMethod(strategyEquals)
                .build();

        final MethodSpec constructor = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC)
                .addParameter(ColumnSource[].class, "keySources")
                .addStatement("super(keySources, $T.INSTANCE)", strategyClass)
                .build();

        final MethodSpec.Builder match = MethodSpec.methodBuilder("match")
                .addAnnotation(Override.class).addModifiers(Modifier.PROTECTED)
                .addParameter(parameter(chunkArrayTypeName(), "keyChunks", notNull))
                .addParameter(
                        parameter(ParameterizedTypeName.get(LongChunk.class, OrderedRowKeys.class), "rowKeys", notNull))
                .addParameter(parameter(ParameterizedTypeName.get(WritableLongChunk.class, OrderedRowKeys.class),
                        "results", notNull))
                .addParameter(boolean.class, "inclusion");
        for (int ii = 0; ii < columns; ++ii) {
            match.addStatement("final $T keys$L = keyChunks[$L].as$LChunk()", chunkTypeName(chunkTypes[ii]), ii, ii,
                    chunkTypes[ii].name());
        }
        match.addStatement("final $T probe = new $T()", probeClass, probeClass);
        match.addStatement("final int size = rowKeys.size()");
        match.beginControlFlow("for (int ii = 0; ii < size; ++ii)");
        for (int ii = 0; ii < columns; ++ii) {
            match.addStatement("probe.k$L = keys$L.get(ii)", ii, ii);
        }
        match.beginControlFlow("if (contains(probe) == inclusion)");
        match.addStatement("results.add(rowKeys.get(ii))");
        match.endControlFlow();
        match.endControlFlow();

        final MethodSpec.Builder exportTuples = MethodSpec.methodBuilder("exportTuples")
                .addAnnotation(Override.class).addModifiers(Modifier.PROTECTED).returns(boolean.class)
                .addParameter(
                        parameter(ParameterizedTypeName.get(ObjectIterator.class, Object.class), "tuples", notNull))
                .addParameter(notNull == null
                        ? ArrayTypeName.of(TypeName.INT)
                        : ArrayTypeName.of(TypeName.INT).annotated(AnnotationSpec.builder(notNull).build()),
                        "columns")
                .addParameter(parameter(ArrayTypeName.of(ParameterizedTypeName.get(ClassName.get(WritableChunk.class),
                        ClassName.get(Values.class))), "keyChunks", notNull));
        // Each tuple column's destination chunk, or null if it is not exported.
        for (int ii = 0; ii < columns; ++ii) {
            exportTuples.addStatement("$T keys$L = null", writableChunkTypeName(chunkTypes[ii]), ii);
        }
        exportTuples.beginControlFlow("for (int ci = 0; ci < columns.length; ++ci)")
                .beginControlFlow("switch (columns[ci])");
        for (int ii = 0; ii < columns; ++ii) {
            exportTuples.addCode("case $L:\n", ii);
            exportTuples.addStatement("keys$L = keyChunks[ci].asWritable$LChunk()", ii, chunkTypes[ii].name());
            exportTuples.addStatement("break");
        }
        exportTuples.addCode("default:\n");
        exportTuples.addStatement("throw new $T($S + columns[ci])", IllegalArgumentException.class,
                "No key column ");
        exportTuples.endControlFlow();
        exportTuples.endControlFlow();
        exportTuples.addStatement("final int capacity = keyChunks[0].capacity()");
        exportTuples.addStatement("int exported = 0");
        exportTuples.beginControlFlow("while (exported < capacity && tuples.hasNext())");
        exportTuples.addStatement("final $T tuple = ($T) tuples.next()", tupleClass, tupleClass);
        for (int ii = 0; ii < columns; ++ii) {
            exportTuples.beginControlFlow("if (keys$L != null)", ii);
            exportTuples.addStatement("keys$L.set(exported, $L)", ii,
                    storedElement(chunkTypes[ii], typedTuple, "tuple", ii));
            exportTuples.endControlFlow();
        }
        exportTuples.addStatement("++exported");
        exportTuples.endControlFlow();
        exportTuples.beginControlFlow("for (final $T keyChunk : keyChunks)",
                ParameterizedTypeName.get(ClassName.get(WritableChunk.class), ClassName.get(Values.class)));
        exportTuples.addStatement("keyChunk.setSize(exported)");
        exportTuples.endControlFlow();
        exportTuples.addStatement("return exported > 0");

        final TypeSpec kernel = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .superclass(TupleMapSetKernel.class)
                .addType(probe.build())
                .addType(strategy)
                .addMethod(constructor)
                .addMethod(hash.build())
                .addMethod(exportTuples.build())
                .addMethod(match.build())
                .build();

        return JavaFile.builder(PACKAGE_NAME, kernel).indent("    ")
                .addFileComment("\n")
                .addFileComment("Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending\n")
                .addFileComment("\n")
                .addFileComment("****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY\n")
                .addFileComment(
                        "****** Run ReplicateTupleSetKernels or ./gradlew replicateTupleSetKernels to regenerate\n")
                .addFileComment("\n")
                .addFileComment("@formatter:off")
                .build();
    }

    /**
     * @return A parameter of {@code type} named {@code name}, annotated with {@code annotation} unless it is null
     */
    private static ParameterSpec parameter(
            @NotNull final TypeName type,
            @NotNull final String name,
            @Nullable final ClassName annotation) {
        final ParameterSpec.Builder parameter = ParameterSpec.builder(type, name);
        if (annotation != null) {
            parameter.addAnnotation(annotation);
        }
        return parameter.build();
    }

    private static TypeName chunkArrayTypeName() {
        return ArrayTypeName.of(ParameterizedTypeName.get(Chunk.class, Values.class));
    }

    private static CodeBlock storedElements(
            @NotNull final ChunkType[] chunkTypes,
            final boolean typedTuple,
            @NotNull final String tupleName) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(ii -> storedElement(chunkTypes[ii], typedTuple, tupleName, ii))
                .collect(CodeBlock.joining(", "));
    }

    private static CodeBlock storedElement(
            @NotNull final ChunkType chunkType,
            final boolean typedTuple,
            @NotNull final String tupleName,
            final int index) {
        if (typedTuple) {
            return CodeBlock.of("$L.get$LElement()", tupleName, ORDINALS[index]);
        }
        if (chunkType == ChunkType.Object) {
            return CodeBlock.of("$L.getElement($L)", tupleName, index);
        }
        // A null element is a boxed null value, which unboxes to the type's null value.
        final Class<?> elementType = elementType(chunkType);
        return CodeBlock.of("$T.unbox(($T) $L.getElement($L))", TypeUtils.class, TypeUtils.getBoxedType(elementType),
                tupleName, index);
    }

    private static CodeBlock equalsExpression(
            @NotNull final ChunkType[] chunkTypes,
            final boolean typedTuple,
            @NotNull final IntFunction<CodeBlock> lhsElement) {
        // Compare the primitive columns first, which are cheap, so that most mismatches never reach an equals call.
        return IntStream.concat(
                IntStream.range(0, chunkTypes.length).filter(ii -> chunkTypes[ii] != ChunkType.Object),
                IntStream.range(0, chunkTypes.length).filter(ii -> chunkTypes[ii] == ChunkType.Object))
                .mapToObj(ii -> CodeBlock.of("$T.eq($L, $L)", comparisons(chunkTypes[ii]), lhsElement.apply(ii),
                        storedElement(chunkTypes[ii], typedTuple, "stored", ii)))
                .collect(CodeBlock.joining(" && "));
    }

    private static ClassName comparisons(@NotNull final ChunkType chunkType) {
        return ClassName.get(COMPARISONS_PACKAGE_NAME, chunkType.name() + "Comparisons");
    }

    private static TypeName chunkTypeName(@NotNull final ChunkType chunkType) {
        final ClassName chunkClass = ClassName.get(Chunk.class.getPackageName(), chunkType.name() + "Chunk");
        return chunkType == ChunkType.Object
                ? ParameterizedTypeName.get(chunkClass, ClassName.get(Object.class), ClassName.get(Values.class))
                : ParameterizedTypeName.get(chunkClass, ClassName.get(Values.class));
    }

    private static TypeName writableChunkTypeName(@NotNull final ChunkType chunkType) {
        final ClassName chunkClass =
                ClassName.get(Chunk.class.getPackageName(), "Writable" + chunkType.name() + "Chunk");
        return chunkType == ChunkType.Object
                ? ParameterizedTypeName.get(chunkClass, ClassName.get(Object.class), ClassName.get(Values.class))
                : ParameterizedTypeName.get(chunkClass, ClassName.get(Values.class));
    }

    private static Class<?> elementType(@NotNull final ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return char.class;
            case Byte:
                return byte.class;
            case Short:
                return short.class;
            case Int:
                return int.class;
            case Long:
                return long.class;
            case Float:
                return float.class;
            case Double:
                return double.class;
            case Object:
                return Object.class;
            default:
                throw new IllegalArgumentException("Unsupported key chunk type " + chunkType);
        }
    }
}
