//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorColumnWrapperTest and run "./gradlew replicateVectorColumnWrappers" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link FloatVectorTest} implementation for {@link io.deephaven.engine.table.vectors.FloatVectorColumnWrapper}.
 */
public class FloatVectorColumnWrapperTest extends FloatVectorTest {

    @Override
    protected FloatVector makeTestVector(@NotNull final float... data) {
        return new FloatVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
