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
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link DoubleVectorTest} implementation for {@link io.deephaven.engine.table.vectors.DoubleVectorColumnWrapper}.
 */
public class DoubleVectorColumnWrapperTest extends DoubleVectorTest {

    @Override
    protected DoubleVector makeTestVector(@NotNull final double... data) {
        return new DoubleVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
