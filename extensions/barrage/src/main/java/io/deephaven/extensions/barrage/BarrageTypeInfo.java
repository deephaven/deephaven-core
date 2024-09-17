//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import org.apache.arrow.flatbuf.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Describes type info used by factory implementations when creating a ChunkReader.
 */
public class BarrageTypeInfo {
    /**
     * Factory method to create a TypeInfo instance.
     *
     * @param type the Java type to be read into the chunk
     * @param componentType the Java type of nested components
     * @param arrowField the Arrow type to be read into the chunk
     * @return a TypeInfo instance
     */
    public static BarrageTypeInfo make(
            @NotNull final Class<?> type,
            @Nullable final Class<?> componentType,
            @NotNull final Field arrowField) {
        return new BarrageTypeInfo(type, componentType, arrowField);
    }

    private final Class<?> type;
    @Nullable
    private final Class<?> componentType;
    private final Field arrowField;

    public BarrageTypeInfo(
            @NotNull final Class<?> type,
            @Nullable final Class<?> componentType,
            @NotNull final Field arrowField) {
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

    public Field arrowField() {
        return arrowField;
    }
}
