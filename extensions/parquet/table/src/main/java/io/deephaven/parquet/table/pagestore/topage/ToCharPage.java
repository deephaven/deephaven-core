//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.materializers.CharMaterializer;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR_BOXED;

public class ToCharPage<ATTR extends Any> implements ToPage<ATTR, char[]> {

    @SuppressWarnings("rawtypes")
    private static final ToCharPage INSTANCE = new ToCharPage<>();

    public static <ATTR extends Any> ToCharPage<ATTR> create(Class<?> nativeType) {
        if (nativeType == null || char.class.equals(nativeType)) {
            // noinspection unchecked
            return INSTANCE;
        }

        throw new IllegalArgumentException("The native type for a Char column is " + nativeType.getCanonicalName());
    }

    private ToCharPage() {}

    @Override
    @NotNull
    public final Class<Character> getNativeType() {
        return char.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    @NotNull
    public final Object nullValue() {
        return NULL_CHAR_BOXED;
    }

    @Override
    @NotNull
    public final PageMaterializerFactory getPageMaterializerFactory() {
        return CharMaterializer.FACTORY;
    }
}
