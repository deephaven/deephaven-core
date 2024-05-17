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
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ShortVectorTest} implementation for {@link io.deephaven.engine.table.vectors.ShortVectorColumnWrapper}.
 */
public class ShortVectorColumnWrapperTest extends ShortVectorTest {

    @Override
    protected ShortVector makeTestVector(@NotNull final short... data) {
        return new ShortVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
