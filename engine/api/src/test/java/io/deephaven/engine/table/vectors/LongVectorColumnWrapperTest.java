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
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link LongVectorTest} implementation for {@link io.deephaven.engine.table.vectors.LongVectorColumnWrapper}.
 */
public class LongVectorColumnWrapperTest extends LongVectorTest {

    @Override
    protected LongVector makeTestVector(@NotNull final long... data) {
        return new LongVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
