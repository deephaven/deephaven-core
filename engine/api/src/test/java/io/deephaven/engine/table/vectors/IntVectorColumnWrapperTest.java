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
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link IntVectorTest} implementation for {@link io.deephaven.engine.table.vectors.IntVectorColumnWrapper}.
 */
public class IntVectorColumnWrapperTest extends IntVectorTest {

    @Override
    protected IntVector makeTestVector(@NotNull final int... data) {
        return new IntVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
