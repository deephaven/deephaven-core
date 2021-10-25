package io.deephaven.engine.v2.utils;

import io.deephaven.test.types.OutOfBandTest;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class MutableRowSetImplTest extends GroupingRowSetHelperTestBase {
    @NotNull
    @Override
    protected TrackingMutableRowSet getSortedIndex(long... keys) {
        final RowSetBuilderRandom treeIndexBuilder = TrackingMutableRowSetImpl.makeCurrentRandomBuilder();
        for (long key : keys) {
            treeIndexBuilder.addKey(key);
        }
        return treeIndexBuilder.build();

    }

    @NotNull
    @Override
    protected final TrackingMutableRowSet fromTreeIndexImpl(@NotNull final TreeIndexImpl treeIndexImpl) {
        return new MutableRowSetImpl(treeIndexImpl);
    }

    @NotNull
    @Override
    protected RowSetFactory getFactory() {
        return RowSetFactoryImpl.INSTANCE;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
