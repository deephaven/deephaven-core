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
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link DoubleVectorTest} implementation for {@link DoubleVectorColumnWrapper}.
 */
public class DoubleVectorColumnWrapperTest extends DoubleVectorTest {

    @Override
    protected DoubleVector makeTestVector(@NotNull final double... data) {
        return new DoubleVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
