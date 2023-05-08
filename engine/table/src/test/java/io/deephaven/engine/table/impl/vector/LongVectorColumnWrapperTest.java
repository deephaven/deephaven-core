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
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link LongVectorTest} implementation for {@link LongVectorColumnWrapper}.
 */
public class LongVectorColumnWrapperTest extends LongVectorTest {

    @Override
    protected LongVector makeTestVector(@NotNull final long... data) {
        return new LongVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
