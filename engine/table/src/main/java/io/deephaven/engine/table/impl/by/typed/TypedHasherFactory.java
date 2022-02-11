package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.*;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.HashHandler;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Modifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Produces typed hashers (e.g. byte * object * float * double * int) on-demand or using a set of pregenerated and
 * precompiled hashers for singleton and pairs of types.
 */
public class TypedHasherFactory {
    private static final boolean USE_PREGENERATED_HASHERS =
            Configuration.getInstance().getBooleanWithDefault("TypedHasherFactory.usePregeneratedHashers", true);

    /**
     * Produce a hasher for the given base class and column sources.
     *
     * @param baseClass the base class (e.g. {@link StaticChunkedOperatorAggregationStateManagerTypedBase)} that the
     *        generated hasher extends from
     * @param tableKeySources the key sources
     * @param tableSize the initial table size
     * @param maximumLoadFactor the maximum load factor of the for the table
     * @param targetLoadFactor the load factor that we will rehash to
     * @param <T> the base class
     * @return an instantiated hasher
     */
    public static <T> T make(Class<T> baseClass, ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        return make(hasherConfigForBase(baseClass), tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }

    @NotNull
    public static <T> HasherConfig<T> hasherConfigForBase(Class<T> baseClass) {
        final String packageMiddle;
        final String mainStateName;
        final String overflowStateName;
        final String emptyStateName;
        if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
            packageMiddle = "staticagg";
            mainStateName = "mainOutputPosition";
            overflowStateName = "overflowOutputPosition";
            emptyStateName = "EMPTY_OUTPUT_POSITION";
        }
        else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
            packageMiddle = "incagg";
            mainStateName = "mainOutputPosition";
            overflowStateName = "overflowOutputPosition";
            emptyStateName = "EMPTY_OUTPUT_POSITION";
        }
        else {
            throw new UnsupportedOperationException("Unknown class to make: " + baseClass);
        }
        final HasherConfig<T> hasherConfig = new HasherConfig<>(baseClass, packageMiddle, mainStateName, overflowStateName, emptyStateName);
        return hasherConfig;
    }

    public static class HasherConfig<T> {
        final Class<T> baseClass;
        public final String packageMiddle;
        final String mainStateName;
        final String overflowStateName;
        final String emptyStateName;

        HasherConfig(Class<T> baseClass, String packageMiddle, String mainStateName, String overflowStateName, String emptyStateName) {
            this.baseClass = baseClass;
            this.packageMiddle = packageMiddle;
            this.mainStateName = mainStateName;
            this.overflowStateName = overflowStateName;
            this.emptyStateName = emptyStateName;
        }
    }

    /**
     * Produce a hasher for the given base class and column sources.
     *
     * @param hasherConfig the configuration of the class to generate
     * @param tableKeySources the key sources
     * @param tableSize the initial table size
     * @param maximumLoadFactor the maximum load factor of the for the table
     * @param targetLoadFactor the load factor that we will rehash to
     * @param <T> the base class
     * @return an instantiated hasher
     */
    public static <T> T make(HasherConfig<T> hasherConfig, ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        final ChunkType[] chunkTypes =
                Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
        if (USE_PREGENERATED_HASHERS) {
            if (hasherConfig.baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.incagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            }
        }

        final String className = hasherName(chunkTypes);

        JavaFile javaFile = generateHasher(hasherConfig, chunkTypes, className, Optional.of(Modifier.PUBLIC));

        String[] javaStrings = javaFile.toString().split("\n");
        final String javaString =
                Arrays.stream(javaStrings).filter(s -> !s.startsWith("package ")).collect(Collectors.joining("\n"));

        final Class<?> clazz = CompilerTools.compile(className, javaString, packageName(hasherConfig.packageMiddle));
        if (!hasherConfig.baseClass.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Generated class is not a " + hasherConfig.baseClass.getCanonicalName());
        }

        final Class<? extends T> castedClass = (Class<? extends T>) clazz;

        T retVal;
        try {
            final Constructor<? extends T> constructor1 =
                    castedClass.getDeclaredConstructor(ColumnSource[].class, int.class, double.class, double.class);
            retVal = constructor1.newInstance(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new UncheckedDeephavenException("Could not instantiate " + castedClass.getCanonicalName(), e);
        }
        return retVal;
    }

    @NotNull
    public static String packageName(String packageMiddle) {
        return "io.deephaven.engine.table.impl.by.typed." + packageMiddle + ".gen";
    }

    @NotNull
    public static String hasherName(ChunkType[] chunkTypes) {
        return "TypedHasher" + Arrays.stream(chunkTypes).map(Objects::toString).collect(Collectors.joining(""));
    }

    @NotNull
    public static <T> JavaFile generateHasher(final HasherConfig<T> hasherConfig,
                                              final ChunkType[] chunkTypes,
                                              final String className,
            Optional<Modifier> visibility) {
        final String packageName = packageName(hasherConfig.packageMiddle);
        final TypeSpec.Builder hasherBuilder =
                TypeSpec.classBuilder(className).addModifiers(Modifier.FINAL).superclass(hasherConfig.baseClass);
        visibility.ifPresent(hasherBuilder::addModifiers);


        CodeBlock.Builder constructorCodeBuilder = CodeBlock.builder();
        constructorCodeBuilder.addStatement("super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        addKeySourceFields(chunkTypes, hasherBuilder, constructorCodeBuilder);

        final MethodSpec constructor =
                MethodSpec.constructorBuilder().addParameter(ColumnSource[].class, "tableKeySources")
                        .addParameter(int.class, "tableSize").addParameter(double.class, "maximumLoadFactor")
                        .addParameter(double.class, "targetLoadFactor").addModifiers(Modifier.PUBLIC)
                        .addCode(constructorCodeBuilder.build())
                        .build();

        final MethodSpec build = createBuildMethod(hasherConfig, chunkTypes);
        final MethodSpec probe = createProbeMethod(hasherConfig, chunkTypes);
        final MethodSpec hash = createHashMethod(chunkTypes);
        final MethodSpec rehashBucket = createRehashBucketMethod(hasherConfig, chunkTypes);
        final MethodSpec maybeMoveMainBucket = createMaybeMoveMainBucket(hasherConfig, chunkTypes);
        final MethodSpec findOverflow = createFindOverflow(chunkTypes);
        final MethodSpec findPositionForKey = createFindPositionForKey(hasherConfig, chunkTypes);

        final TypeSpec hasher =
                hasherBuilder.addMethod(constructor).addMethod(build).addMethod(probe).addMethod(hash)
                        .addMethod(rehashBucket)
                        .addMethod(maybeMoveMainBucket).addMethod(findOverflow).addMethod(findPositionForKey).build();

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, hasher).indent("    ");
        fileBuilder.addFileComment("DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY "
                + TypedHasherFactory.class.getCanonicalName() + "\n" +
                "Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n");

        for (ChunkType chunkType : chunkTypes) {
            fileBuilder.addStaticImport(
                    ClassName.get(CharComparisons.class.getPackageName(), chunkType.name() + "Comparisons"), "eq");
        }
        return fileBuilder.build();
    }

    private static void addKeySourceFields(ChunkType[] chunkTypes, TypeSpec.Builder hasherBuilder,
            CodeBlock.Builder constructorCodeBuilder) {
        final List<FieldSpec> keySources = new ArrayList<>();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            Class<?> type = arraySourceType(chunkTypes[ii]);
            keySources.add(
                    FieldSpec.builder(type, "mainKeySource" + ii).addModifiers(Modifier.PRIVATE, Modifier.FINAL).build());
            keySources.add(FieldSpec.builder(type, "overflowKeySource" + ii)
                    .addModifiers(Modifier.PRIVATE, Modifier.FINAL).build());
            constructorCodeBuilder.addStatement("this.mainKeySource$L = ($T) super.mainKeySources[$L]", ii, type, ii);
            constructorCodeBuilder.addStatement("this.overflowKeySource$L = ($T) super.overflowKeySources[$L]", ii,
                    type, ii);
        }
        keySources.forEach(hasherBuilder::addField);
    }

    @NotNull
    private static MethodSpec createFindOverflow(ChunkType[] chunkTypes) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("findOverflow").addParameter(HashHandler.class, "handler");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addParameter(elementType(chunkTypes[ii]), "k" + ii);
        }

        builder.addParameter(int.class, "chunkPosition").addParameter(int.class, "overflowLocation")
                .returns(boolean.class).addModifiers(Modifier.PRIVATE)
                .beginControlFlow("while (overflowLocation != $T.NULL_INT)", QueryConstants.class)
                .beginControlFlow("if (" + getEqualsStatementOverflow(chunkTypes) + ")")
                .addStatement("handler.doOverflowFound(overflowLocation, chunkPosition)")
                .addStatement("return true")
                .endControlFlow()
                .addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)")
                .endControlFlow()
                .addStatement("return false");
        return builder.build();
    }

    @NotNull
    private static MethodSpec createFindPositionForKey(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("findPositionForKey").addParameter(Object.class, "key")
                .returns(int.class).addModifiers(Modifier.PUBLIC).addAnnotation(Override.class);
        if (chunkTypes.length != 1) {
            builder.addStatement("final Object [] ka = (Object[])key");
            for (int ii = 0; ii < chunkTypes.length; ++ii) {
                final Class<?> element = elementType(chunkTypes[ii]);
                unboxKey(builder, ii, element);
            }
        } else {
            final Class<?> element = elementType(chunkTypes[0]);
            unboxKey(builder, element);
        }

        builder.addStatement("int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.addStatement("final int positionValue = $L.getUnsafe(tableLocation)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (positionValue == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return -1");
        builder.endControlFlow();

        builder.beginControlFlow("if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("return positionValue");
        builder.endControlFlow();

        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.beginControlFlow("if (" + getEqualsStatementOverflow(chunkTypes) + ")");
        builder.addStatement("return $L.getUnsafe(overflowLocation)", hasherConfig.overflowStateName);
        builder.endControlFlow();

        builder.addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)");
        builder.endControlFlow();
        builder.addStatement("return -1");
        return builder.build();
    }

    private static void unboxKey(MethodSpec.Builder builder, int ii, Class<?> element) {
        if (element == Object.class) {
            builder.addStatement("final $T k$L = ka[$L]", element, ii, ii);
        } else {
            builder.addStatement("final $T k$L = $T.unbox(($T)ka[$L])", element, ii, TypeUtils.class,
                    TypeUtils.getBoxedType(element), ii);
        }
    }

    private static void unboxKey(MethodSpec.Builder builder, Class<?> element) {
        if (element == Object.class) {
            builder.addStatement("final $T k0 = key", element);
        } else {
            builder.addStatement("final $T k0 = $T.unbox(($T)key)", element, TypeUtils.class,
                    TypeUtils.getBoxedType(element));
        }
    }

    @NotNull
    private static MethodSpec createMaybeMoveMainBucket(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        MethodSpec.Builder builder =
                MethodSpec.methodBuilder("maybeMoveMainBucket").addParameter(HashHandler.class, "handler")
                        .addParameter(int.class, "sourceBucket").addParameter(int.class, "destBucket")
                        .addParameter(int.class, "bucketsToAdd").returns(int.class).addModifiers(Modifier.PRIVATE);

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = mainKeySource$L.getUnsafe(sourceBucket)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash)");
        builder.addStatement("final int mainInsertLocation");
        builder.beginControlFlow("if (location == sourceBucket)");
        builder.addStatement("mainInsertLocation = destBucket");
        builder.addStatement("$L.set(destBucket, $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.nextControlFlow("else");
        builder.addStatement("mainInsertLocation = sourceBucket");
        builder.addStatement("$L.set(destBucket, $L.getUnsafe(sourceBucket))", hasherConfig.mainStateName, hasherConfig.mainStateName);
        builder.addStatement("$L.set(sourceBucket, $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(destBucket, k$L)", ii, ii);
            builder.addStatement("mainKeySource$L.set(sourceBucket, $L)", ii, elementNull(chunkTypes[ii]));
        }
        builder.addStatement("handler.doMoveMain(sourceBucket, destBucket)");
        builder.endControlFlow();
        builder.addStatement("return mainInsertLocation");

        return builder
                .build();
    }

    @NotNull
    private static MethodSpec createRehashBucketMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("final int position = $L.getUnsafe(sourceBucket)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (position == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return");
        builder.endControlFlow();

        builder.addStatement("int mainInsertLocation = maybeMoveMainBucket(handler, sourceBucket, destBucket, bucketsToAdd)");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(sourceBucket)");
        builder.addStatement("mainOverflowLocationSource.set(sourceBucket, QueryConstants.NULL_INT)");
        builder.addStatement("mainOverflowLocationSource.set(destBucket, QueryConstants.NULL_INT)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.addStatement(
                "final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T overflowKey$L = overflowKeySource$L.getUnsafe(overflowLocation)",
                    elementType(chunkTypes[ii]), ii, ii);
        }
        builder.addStatement("final int overflowHash = hash(" + IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "overflowKey" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement(
                "final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash)");
        builder.beginControlFlow("if (overflowTableLocation == mainInsertLocation)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(mainInsertLocation, overflowKey$L)", ii, ii);
        }
        builder.addStatement("$L.set(mainInsertLocation, $L.getUnsafe(overflowLocation))", hasherConfig.mainStateName, hasherConfig.overflowStateName);
        builder.addStatement("handler.doPromoteOverflow(overflowLocation, mainInsertLocation)");
        builder.addStatement("$L.set(overflowLocation, QueryConstants.NULL_INT)", hasherConfig.overflowStateName);

        // key source loop
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(overflowLocation, $L)", ii, elementNull(chunkTypes[ii]));
        }

        builder.addStatement("freeOverflowLocation(overflowLocation)");
        builder.addStatement("mainInsertLocation = -1");
        builder.nextControlFlow("else");
        builder.addStatement("final int oldOverflowLocation = mainOverflowLocationSource.getUnsafe(overflowTableLocation)");
        builder.addStatement("mainOverflowLocationSource.set(overflowTableLocation, overflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation)");
        builder.endControlFlow();
        builder.addStatement("overflowLocation = nextOverflowLocation");
        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashBucket").addParameter(HashHandler.class, "handler")
                .addParameter(int.class, "sourceBucket").addParameter(int.class, "destBucket")
                .addParameter(int.class, "bucketsToAdd").returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createBuildMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if ($L.getUnsafe(tableLocation) == $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("numEntries++");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(tableLocation, k$L)", ii, ii);
        }
        builder.addStatement("handler.doMainInsert(tableLocation, chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("final int newOverflowLocation = allocateOverflowLocation()");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(newOverflowLocation, k$L)", ii, ii);
        }
        builder.addStatement("mainOverflowLocationSource.set(tableLocation, newOverflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation)");
        builder.addStatement("numEntries++");
        builder.addStatement("handler.doOverflowInsert(newOverflowLocation, chunkPosition)");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();


        return MethodSpec.methodBuilder("build")
                .addParameter(HashHandler.class, "handler")
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createProbeMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if ($L.getUnsafe(tableLocation) == $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();

        return MethodSpec.methodBuilder("probe")
                .addParameter(HashHandler.class, "handler")
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createHashMethod(ChunkType[] chunkTypes) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("hash").returns(int.class).addModifiers(Modifier.PRIVATE, Modifier.STATIC);
        final CodeBlock.Builder hashCodeBuilder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ChunkType chunkType = chunkTypes[ii];
            builder.addParameter(elementType(chunkType), "k" + ii);
            if (ii == 0) {
                hashCodeBuilder.addStatement("int hash = $T.hashInitialSingle(k0)", chunkHasherByChunkType(chunkType));
            } else {
                hashCodeBuilder.addStatement("hash = $T.hashUpdateSingle(hash, k" + ii + ")",
                        chunkHasherByChunkType(chunkType));
            }
        }
        hashCodeBuilder.addStatement("return hash");
        return builder.addCode(hashCodeBuilder.build()).build();
    }

    @NotNull
    private static String getEqualsStatement(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(mainKeySource" + x + ".getUnsafe(tableLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    @NotNull
    private static String getEqualsStatementOverflow(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(overflowKeySource" + x + ".getUnsafe(overflowLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    private static ClassName chunkHasherByChunkType(ChunkType chunkType) {
        return ClassName.get(CharChunkHasher.class.getPackageName(), chunkType.name() + "ChunkHasher");
    }

    static Class<? extends ArrayBackedColumnSource> arraySourceType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return CharacterArraySource.class;
            case Byte:
                return ByteArraySource.class;
            case Short:
                return ShortArraySource.class;
            case Int:
                return IntegerArraySource.class;
            case Long:
                return LongArraySource.class;
            case Float:
                return FloatArraySource.class;
            case Double:
                return DoubleArraySource.class;
            case Object:
                return ObjectArraySource.class;
        }
    }

    static Class<?> elementType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
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
        }
    }

    private static String elementNull(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
            case Byte:
            case Short:
            case Int:
            case Long:
            case Float:
            case Double:
                return "QueryConstants.NULL_" + chunkType.name().toUpperCase();
            case Object:
                return "null";
        }
    }
}
