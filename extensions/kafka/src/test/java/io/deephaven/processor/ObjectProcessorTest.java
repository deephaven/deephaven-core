/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ChunkType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.Type;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectProcessorTest {

    interface Foo {
    }

    @Test
    public void testChunkTypes() {
        chunkType(Type.charType(), ChunkType.Char);
        chunkType(Type.booleanType(), ChunkType.Byte);
        chunkType(Type.byteType(), ChunkType.Byte);
        chunkType(Type.shortType(), ChunkType.Short);
        chunkType(Type.intType(), ChunkType.Int);
        chunkType(Type.longType(), ChunkType.Long);
        chunkType(Type.floatType(), ChunkType.Float);
        chunkType(Type.doubleType(), ChunkType.Double);

        chunkType(Type.charType().boxedType(), ChunkType.Char);
        chunkType(Type.booleanType().boxedType(), ChunkType.Byte);
        chunkType(Type.byteType().boxedType(), ChunkType.Byte);
        chunkType(Type.shortType().boxedType(), ChunkType.Short);
        chunkType(Type.intType().boxedType(), ChunkType.Int);
        chunkType(Type.longType().boxedType(), ChunkType.Long);
        chunkType(Type.floatType().boxedType(), ChunkType.Float);
        chunkType(Type.doubleType().boxedType(), ChunkType.Double);

        chunkType(Type.stringType(), ChunkType.Object);
        chunkType(Type.instantType(), ChunkType.Long);
        chunkType(Type.ofCustom(Foo.class), ChunkType.Object);

        PrimitiveType.instances()
                .map(Type::arrayType)
                .forEach(nativeArrayType -> chunkType(nativeArrayType, ChunkType.Object));
    }

    private static void chunkType(Type<?> type, ChunkType expectedChunkType) {
        assertThat(ObjectProcessor.chunkType(type)).isSameAs(expectedChunkType);
    }
}
