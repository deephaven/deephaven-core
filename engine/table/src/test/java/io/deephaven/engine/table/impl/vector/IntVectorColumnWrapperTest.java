/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link IntVectorTest} implementation for {@link IntVectorColumnWrapper}.
 */
public class IntVectorColumnWrapperTest extends IntVectorTest {

    @Override
    protected IntVector makeTestVector(@NotNull final int... data) {
        return new IntVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
