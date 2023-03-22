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
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ByteVectorTest} implementation for {@link ByteVectorColumnWrapper}.
 */
public class ByteVectorColumnWrapperTest extends ByteVectorTest {

    @Override
    protected ByteVector makeTestVector(@NotNull final byte... data) {
        return new ByteVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(data),
                RowSetFactory.flat(data.length));
    }
}
