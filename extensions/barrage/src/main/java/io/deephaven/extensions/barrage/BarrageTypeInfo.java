//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Describes type info used by factory implementations when creating a ChunkReader.
 */
public class BarrageTypeInfo<FIELD_TYPE> {
    /**
     * Factory method to create a TypeInfo instance.
     *
     * @param type the Java type to be read into the chunk
     * @param componentType the Java type of nested components
     * @param arrowField the Arrow type to be read into the chunk
     * @return a TypeInfo instance
     */
    public static <FIELD_TYPE> BarrageTypeInfo<FIELD_TYPE> make(
            @NotNull final Class<?> type,
            @Nullable final Class<?> componentType,
            @NotNull final FIELD_TYPE arrowField) {
        return new BarrageTypeInfo<>(type, componentType, arrowField);
    }

    private final Class<?> type;
    @Nullable
    private final Class<?> componentType;
    private final FIELD_TYPE arrowField;

    public BarrageTypeInfo(
            @NotNull final Class<?> type,
            @Nullable final Class<?> componentType,
            @NotNull final FIELD_TYPE arrowField) {
        this.type = type;
        this.componentType = componentType;
        this.arrowField = arrowField;
    }

    public Class<?> type() {
        return type;
    }

    @Nullable
    public Class<?> componentType() {
        return componentType;
    }

    public FIELD_TYPE arrowField() {
        return arrowField;
    }

    public ChunkType chunkType() {
        if (type == boolean.class || type == Boolean.class) {
            return ChunkType.Byte;
        }
        if (!type.isPrimitive()) {
            return ChunkType.Object;
        }
        return ChunkType.fromElementType(type);
    }
}
