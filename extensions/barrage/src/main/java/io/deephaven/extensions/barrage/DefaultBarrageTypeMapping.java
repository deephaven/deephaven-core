//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

public class DefaultBarrageTypeMapping implements BarrageTypeMapping {

    @Override
    public ArrowType mapToArrowType(
            @NotNull final Class<?> type,
            @NotNull final Class<?> componentType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public BarrageTypeInfo mapFromArrowType(
            @NotNull final ArrowType arrowType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Schema schemaFrom(
            @NotNull final BarrageTableDefinition tableDefinition) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public BarrageTableDefinition tableDefinitionFrom(
            @NotNull final Schema schema) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
