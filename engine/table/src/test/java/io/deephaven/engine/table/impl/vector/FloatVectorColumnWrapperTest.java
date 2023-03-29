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
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link FloatVectorTest} implementation for {@link FloatVectorColumnWrapper}.
 */
public class FloatVectorColumnWrapperTest extends FloatVectorTest {

    @Override
    protected FloatVector makeTestVector(@NotNull final float... data) {
        return new FloatVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
