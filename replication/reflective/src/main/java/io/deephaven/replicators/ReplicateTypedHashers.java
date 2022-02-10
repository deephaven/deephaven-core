package io.deephaven.replicators;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.IncrementalChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;
import io.deephaven.engine.table.impl.by.typed.TypeChunkedHashFactory;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class ReplicateTypedHashers {
    public static void main(String[] args) throws IOException {
        generatePackage("staticagg", StaticChunkedOperatorAggregationStateManagerTypedBase.class);
        generatePackage("incagg", IncrementalChunkedOperatorAggregationStateManagerTypedBase.class);
    }

    private static void generatePackage(String packageMiddle, Class<?> baseClass) throws IOException {
        final String packageName = TypeChunkedHashFactory.packageName(packageMiddle);
        final File sourceRoot = new File("engine/table/src/main/java/");

        final MethodSpec.Builder singleDispatchBuilder = MethodSpec.methodBuilder("dispatchSingle")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .addParameter(ChunkType.class, "chunkType");
        addHasherParametersAndReturnType(singleDispatchBuilder, baseClass);

        singleDispatchBuilder.beginControlFlow("switch (chunkType)");
        singleDispatchBuilder.addCode("default:");
        singleDispatchBuilder.addStatement("throw new UnsupportedOperationException($S + chunkType)",
                "Invalid chunk type for typed hashers: ");

        final ChunkType[] array = new ChunkType[1];
        for (ChunkType chunkType : ChunkType.values()) {
            if (chunkType == ChunkType.Boolean) {
                continue;
            }
            array[0] = chunkType;
            final String name = TypeChunkedHashFactory.hasherName(array);
            final JavaFile javaFile =
                    TypeChunkedHashFactory.generateHasher(baseClass, array, name, packageName, Optional.empty());

            System.out.println("Generating " + name + " to " + sourceRoot);
            javaFile.writeTo(sourceRoot);
            singleDispatchBuilder.addCode("case " + chunkType.name() + ":");
            singleDispatchBuilder.addStatement(
                    "return new $T(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)",
                    ClassName.get(packageName, name));
        }

        singleDispatchBuilder.endControlFlow();

        final MethodSpec.Builder doubleDispatchBuilder = MethodSpec.methodBuilder("dispatchDouble")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .addParameter(ChunkType.class, "chunkType0")
                .addParameter(ChunkType.class, "chunkType1");
        addHasherParametersAndReturnType(doubleDispatchBuilder, baseClass);

        doubleDispatchBuilder.beginControlFlow("switch (chunkType0)");
        doubleDispatchBuilder.addCode("default:");
        doubleDispatchBuilder.addStatement("throw new UnsupportedOperationException($S + chunkType0)",
                "Invalid chunk type for typed hashers: ");

        final ChunkType[] array2 = new ChunkType[2];
        for (ChunkType chunkType0 : ChunkType.values()) {
            if (chunkType0 == ChunkType.Boolean) {
                continue;
            }
            array2[0] = chunkType0;

            doubleDispatchBuilder.addCode("case " + chunkType0.name() + ":");
            doubleDispatchBuilder.beginControlFlow("switch (chunkType1)");
            doubleDispatchBuilder.addCode("default:");
            doubleDispatchBuilder.addStatement("throw new UnsupportedOperationException($S + chunkType1)",
                    "Invalid chunk type for typed hashers: ");

            for (ChunkType chunkType1 : ChunkType.values()) {
                if (chunkType1 == ChunkType.Boolean) {
                    continue;
                }

                array2[1] = chunkType1;

                final String name = TypeChunkedHashFactory.hasherName(array2);
                final JavaFile javaFile =
                        TypeChunkedHashFactory.generateHasher(baseClass, array2, name, packageName, Optional.empty());

                System.out.println("Generating " + name + " to " + sourceRoot);
                javaFile.writeTo(sourceRoot);

                doubleDispatchBuilder.addCode("case " + chunkType1.name() + ":");
                doubleDispatchBuilder.addStatement(
                        "return new $T(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)",
                        ClassName.get(packageName, name));
            }
            doubleDispatchBuilder.endControlFlow();
        }
        doubleDispatchBuilder.endControlFlow();

        final MethodSpec.Builder dispatchMethodBuilder = MethodSpec.methodBuilder("dispatch")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC);
        addHasherParametersAndReturnType(dispatchMethodBuilder, baseClass);
        dispatchMethodBuilder.addStatement(
                "final ChunkType[] chunkTypes = $T.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);",
                java.util.Arrays.class);
        dispatchMethodBuilder.beginControlFlow("if (chunkTypes.length == 1)");
        dispatchMethodBuilder.addStatement(
                "return dispatchSingle(chunkTypes[0], tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        dispatchMethodBuilder.endControlFlow();
        dispatchMethodBuilder.beginControlFlow("if (chunkTypes.length == 2)");
        dispatchMethodBuilder.addStatement(
                "return dispatchDouble(chunkTypes[0], chunkTypes[1], tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        dispatchMethodBuilder.endControlFlow();
        dispatchMethodBuilder.addStatement("return null");


        final String dispatcherName = "TypedHashDispatcher";
        final TypeSpec.Builder dispatchBuilder =
                TypeSpec.classBuilder(dispatcherName).addModifiers(Modifier.PUBLIC).addJavadoc(
                        "The TypedHashDispatcher returns a pre-generated and precompiled hasher instance suitable for the provided column sources, or null if there is not a precompiled hasher suitable for the specified sources.");
        dispatchBuilder.addMethod(
                MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).addComment("static use only").build());
        dispatchBuilder.addMethod(dispatchMethodBuilder.build());
        dispatchBuilder.addMethod(singleDispatchBuilder.build());
        dispatchBuilder.addMethod(doubleDispatchBuilder.build());

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, dispatchBuilder.build());
        fileBuilder.addFileComment("DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY "
                + ReplicateTypedHashers.class.getCanonicalName() + "\n" +
                "Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n");
        final JavaFile dispatcherFile = fileBuilder.build();
        dispatcherFile.writeTo(sourceRoot);
    }

    private static void addHasherParametersAndReturnType(MethodSpec.Builder doubleDispatchBuilder,
            Class<?> returnType) {
        doubleDispatchBuilder
                .returns(returnType)
                .addParameter(ColumnSource[].class, "tableKeySources")
                .addParameter(int.class, "tableSize")
                .addParameter(double.class, "maximumLoadFactor")
                .addParameter(double.class, "targetLoadFactor");
    }
}
