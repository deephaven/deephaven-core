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
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Modifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TypeChunkedHashFactory {
    private static final boolean USE_PREGENERATED_HASHERS =
            Configuration.getInstance().getBooleanWithDefault("TypeChunkedHashFactory.usePregeneratedHashers", true);

    public static <T> T make(String packageMiddle, Class<T> baseClass, ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        final ChunkType[] chunkTypes =
                Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
        if (USE_PREGENERATED_HASHERS) {
            if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.incagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            }
        }

        final String className = hasherName(chunkTypes);

        JavaFile javaFile = generateHasher(baseClass, chunkTypes, className, packageName(packageMiddle));

        String[] javaStrings = javaFile.toString().split("\n");
        final String javaString =
                Arrays.stream(javaStrings).filter(s -> !s.startsWith("package ")).collect(Collectors.joining("\n"));

        final Class<?> clazz = CompilerTools.compile(className, javaString, packageName(packageMiddle));
        if (!baseClass.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Generated class is not a " + baseClass.getCanonicalName());
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
    public static <T> JavaFile generateHasher(Class<T> baseClass, ChunkType[] chunkTypes, String className,
            String packageName) {
        final TypeSpec.Builder hasherBuilder =
                TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC, Modifier.FINAL).superclass(baseClass);


        CodeBlock.Builder constructorCodeBuilder = CodeBlock.builder();
        constructorCodeBuilder.addStatement("super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        addKeySourceFields(chunkTypes, hasherBuilder, constructorCodeBuilder);

        final MethodSpec constructor =
                MethodSpec.constructorBuilder().addParameter(ColumnSource[].class, "tableKeySources")
                        .addParameter(int.class, "tableSize").addParameter(double.class, "maximumLoadFactor")
                        .addParameter(double.class, "targetLoadFactor").addModifiers(Modifier.PUBLIC)
                        .addCode(constructorCodeBuilder.build())
                        .build();

        final MethodSpec build = createBuildMethod(chunkTypes);
        final MethodSpec probe = createProbeMethod(chunkTypes);
        final MethodSpec hash = createHashMethod(chunkTypes);
        final MethodSpec rehashBucket = createRehashBucketMethod(chunkTypes);
        final MethodSpec maybeMoveMainBucket = createMaybeMoveMainBucket(chunkTypes);
        final MethodSpec findOverflow = createFindOverflow(chunkTypes);
        final MethodSpec findPositionForKey = createFindPositionForKey(chunkTypes);

        final TypeSpec hasher =
                hasherBuilder.addMethod(constructor).addMethod(build).addMethod(probe).addMethod(hash)
                        .addMethod(rehashBucket)
                        .addMethod(maybeMoveMainBucket).addMethod(findOverflow).addMethod(findPositionForKey).build();

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, hasher);
        fileBuilder.addFileComment("DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY "
                + TypeChunkedHashFactory.class.getCanonicalName() + "\n" +
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
                    FieldSpec.builder(type, "keySource" + ii).addModifiers(Modifier.PRIVATE, Modifier.FINAL).build());
            keySources.add(FieldSpec.builder(type, "overflowKeySource" + ii)
                    .addModifiers(Modifier.PRIVATE, Modifier.FINAL).build());
            constructorCodeBuilder.addStatement("this.keySource$L = ($T) super.keySources[$L]", ii, type, ii);
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
            builder.addParameter(elementType(chunkTypes[ii]), "v" + ii);
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
    private static MethodSpec createFindPositionForKey(ChunkType[] chunkTypes) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("findPositionForKey").addParameter(Object.class, "value")
                .returns(int.class).addModifiers(Modifier.PUBLIC).addAnnotation(Override.class);
        if (chunkTypes.length != 1) {
            builder.addStatement("final Object [] va = (Object[])value");
            for (int ii = 0; ii < chunkTypes.length; ++ii) {
                final Class<?> element = elementType(chunkTypes[ii]);
                builder.addStatement("final $T v$L = ($T)va[$L]", element, ii, element, ii);
            }
        } else {
            final Class<?> element = elementType(chunkTypes[0]);
            builder.addStatement("final $T v0 = ($T)value", element, element);
        }

        builder.addStatement("int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "v" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.addStatement("final int positionValue = stateSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (positionValue == EMPTY_RIGHT_VALUE)");
        builder.addStatement("return -1");
        builder.endControlFlow();

        builder.beginControlFlow("if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("return positionValue");
        builder.endControlFlow();

        builder.addStatement("int overflowLocation = overflowLocationSource.getUnsafe(tableLocation)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.beginControlFlow("if (" + getEqualsStatementOverflow(chunkTypes) + ")");
        builder.addStatement("return overflowStateSource.getUnsafe(overflowLocation)");
        builder.endControlFlow();

        builder.addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation);");
        builder.endControlFlow();
        builder.addStatement("return -1");
        return builder.build();
    }

    @NotNull
    private static MethodSpec createMaybeMoveMainBucket(ChunkType[] chunkTypes) {
        MethodSpec.Builder builder =
                MethodSpec.methodBuilder("maybeMoveMainBucket").addParameter(HashHandler.class, "handler")
                        .addParameter(int.class, "bucket").addParameter(int.class, "destBucket")
                        .addParameter(int.class, "bucketsToAdd").returns(int.class).addModifiers(Modifier.PRIVATE);

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T v$L = keySource$L.getUnsafe(bucket)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "v" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash)");
        builder.addStatement("final int mainInsertLocation");
        builder.beginControlFlow("if (location == bucket)");
        builder.addStatement("mainInsertLocation = destBucket");
        builder.addStatement("stateSource.set(destBucket, EMPTY_RIGHT_VALUE)");
        builder.nextControlFlow("else");
        builder.addStatement("mainInsertLocation = bucket");
        builder.addStatement("stateSource.set(destBucket, stateSource.getUnsafe(bucket))");
        builder.addStatement("stateSource.set(bucket, EMPTY_RIGHT_VALUE)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("keySource$L.set(destBucket, v$L)", ii, ii);
            builder.addStatement("keySource$L.set(bucket, $L)", ii, elementNull(chunkTypes[ii]));
        }
        builder.addStatement("handler.moveMain(bucket, destBucket)");
        builder.endControlFlow();
        builder.addStatement("return mainInsertLocation");

        return builder
                .build();
    }

    @NotNull
    private static MethodSpec createRehashBucketMethod(ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("final int position = stateSource.getUnsafe(bucket)");
        builder.beginControlFlow("if (position == EMPTY_RIGHT_VALUE)");
        builder.addStatement("return");
        builder.endControlFlow();

        builder.addStatement("int mainInsertLocation = maybeMoveMainBucket(handler, bucket, destBucket, bucketsToAdd)");
        builder.addStatement("int overflowLocation = overflowLocationSource.getUnsafe(bucket)");
        builder.addStatement("overflowLocationSource.set(bucket, QueryConstants.NULL_INT)");
        builder.addStatement("overflowLocationSource.set(destBucket, QueryConstants.NULL_INT)");

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
            builder.addStatement("keySource$L.set(mainInsertLocation, overflowKey$L)", ii, ii);
        }
        builder.addStatement("stateSource.set(mainInsertLocation, overflowStateSource.getUnsafe(overflowLocation))");
        builder.addStatement("handler.promoteOverflow(overflowLocation, mainInsertLocation)");
        builder.addStatement("overflowStateSource.set(overflowLocation, QueryConstants.NULL_INT)");

        // key source loop
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(overflowLocation, $L)", ii, elementNull(chunkTypes[ii]));
        }

        builder.addStatement("freeOverflowLocation(overflowLocation)");
        builder.addStatement("mainInsertLocation = -1");
        builder.nextControlFlow("else");
        builder.addStatement("final int oldOverflowLocation = overflowLocationSource.getUnsafe(overflowTableLocation)");
        builder.addStatement("overflowLocationSource.set(overflowTableLocation, overflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation)");
        builder.endControlFlow();
        builder.addStatement("overflowLocation = nextOverflowLocation");
        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashBucket").addParameter(HashHandler.class, "handler")
                .addParameter(int.class, "bucket").addParameter(int.class, "destBucket")
                .addParameter(int.class, "bucketsToAdd").returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createBuildMethod(ChunkType[] chunkTypes) {
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
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T v$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "v" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if (stateSource.getUnsafe(tableLocation) == EMPTY_RIGHT_VALUE)");
        builder.addStatement("numEntries++");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("keySource$L.set(tableLocation, v$L)", ii, ii);
        }
        builder.addStatement("handler.doMainInsert(tableLocation, chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = overflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "v" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("final int newOverflowLocation = allocateOverflowLocation()");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(newOverflowLocation, v$L)", ii, ii);
        }
        builder.addStatement("overflowLocationSource.set(tableLocation, newOverflowLocation)");
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
    private static MethodSpec createProbeMethod(ChunkType[] chunkTypes) {
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
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < keyChunk0.size(); ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T v$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "v" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if (stateSource.getUnsafe(tableLocation) == EMPTY_RIGHT_VALUE)");
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = overflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "v" + x).collect(Collectors.joining(", "))
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
                MethodSpec.methodBuilder("hash").returns(int.class).addModifiers(Modifier.PRIVATE);
        final CodeBlock.Builder hashCodeBuilder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ChunkType chunkType = chunkTypes[ii];
            builder.addParameter(elementType(chunkType), "v" + ii);
            if (ii == 0) {
                hashCodeBuilder.addStatement("int hash = $T.hashInitialSingle(v0)", chunkHasherByChunkType(chunkType));
            } else {
                hashCodeBuilder.addStatement("hash = $T.hashUpdateSingle(hash, v" + ii + ")",
                        chunkHasherByChunkType(chunkType));
            }
        }
        hashCodeBuilder.addStatement("return hash");
        return builder.addCode(hashCodeBuilder.build()).build();
    }

    @NotNull
    private static String getEqualsStatement(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(keySource" + x + ".getUnsafe(tableLocation), v" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    @NotNull
    private static String getEqualsStatementOverflow(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(overflowKeySource" + x + ".getUnsafe(overflowLocation), v" + x + ")")
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
