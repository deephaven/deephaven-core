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
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorTest;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ObjectVectorTest} implementation for {@link io.deephaven.engine.table.vectors.ObjectVectorColumnWrapper}.
 */
public class ObjectVectorColumnWrapperTest extends ObjectVectorTest {

    @Override
    protected ObjectVector<Object> makeTestVector(@NotNull final Object... data) {
        return new ObjectVectorColumnWrapper<>(
                ArrayBackedColumnSource.getMemoryColumnSource(data, Object.class, null),
                RowSetFactory.flat(data.length));
    }
}
