//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.vectors;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link CharVectorTest} implementation for {@link io.deephaven.engine.table.vectors.CharVectorColumnWrapper}.
 */
public class CharVectorColumnWrapperTest extends CharVectorTest {

    @Override
    protected CharVector makeTestVector(@NotNull final char... data) {
        return new CharVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
