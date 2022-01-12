package io.deephaven.engine.table.impl.sources;

import org.jetbrains.annotations.NotNull;

public class TestObjectSparseArraySource extends AbstractObjectColumnSourceTest {
    @NotNull
    @Override
    ObjectSparseArraySource<Object> makeTestSource() {
        return new ObjectSparseArraySource(String.class);
    }
}