package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.StaticChunkedOperatorAggregationStateManagerTypedBase;

import javax.lang.model.element.Modifier;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;

public class TypedStaticChunkedOperatorAggregationStateManager {
    public static StaticChunkedOperatorAggregationStateManagerTypedBase make(ColumnSource<?>[] tableKeySources
            , int tableSize
            , double maximumLoadFactor
            , double targetLoadFactor) {
        final ChunkType[] chunkTypes = Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
        if (chunkTypes.length == 1) {
            final ChunkType chunkType = chunkTypes[0];
            if (chunkType == ChunkType.Char) {
                return new CharTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Byte) {
                return new ByteTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Short) {
                return new ShortTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Int) {
                return new IntTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Long) {
                return new LongTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Float) {
                return new FloatTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Double) {
                return new DoubleTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
            if (chunkType == ChunkType.Object) {
                return new ObjectTypedHasher(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
            }
        }

        final String className = "TypedHasher" + Arrays.stream(chunkTypes).map(Objects::toString);
        final String packageName = "io.deephaven.engine.table.impl.by.typed.gen";

        final TypeSpec hasher = TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC, Modifier.FINAL).superclass(StaticChunkedOperatorAggregationStateManagerTypedBase.class).build();

        JavaFile javaFile = JavaFile.builder(packageName, hasher).build();

        final String javaString = javaFile.toString();

        System.out.println(javaString);

        final Class<?> clazz = CompilerTools.compile(className, javaString, packageName);
        if (!StaticChunkedOperatorAggregationStateManagerTypedBase.class.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Generated class is not a StaticChunkedOperatorAggregationStateManagerNonVector!");
        }

        final Class<? extends StaticChunkedOperatorAggregationStateManagerTypedBase> castedClass = (Class<? extends StaticChunkedOperatorAggregationStateManagerTypedBase>)clazz;

        StaticChunkedOperatorAggregationStateManagerTypedBase retVal;
        try {
            retVal = castedClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new UncheckedDeephavenException("Could not instantiate StaticChunkedOperatorAggregationStateManagerNonVector!", e);
        }
        return retVal;
    }
}
