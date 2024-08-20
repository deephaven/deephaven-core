//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import org.jetbrains.annotations.NotNull;

public class TestObjectSparseArraySource extends AbstractObjectColumnSourceTest {
    @NotNull
    @Override
    ObjectSparseArraySource<Object> makeTestSource() {
        return new ObjectSparseArraySource(String.class);
    }
}
