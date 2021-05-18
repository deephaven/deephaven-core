package io.deephaven.db.v2.utils;

import io.deephaven.test.types.OutOfBandTest;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class CurrentOnlyIndexTest extends SortedIndexTestBase {
    @NotNull
    @Override
    protected Index getSortedIndex(long... keys) {
        final Index.RandomBuilder treeIndexBuilder = TreeIndex.makeCurrentRandomBuilder();
        for (long key : keys) {
            treeIndexBuilder.addKey(key);
        }
        return treeIndexBuilder.getIndex();

    }

    @NotNull
    @Override
    protected final Index fromTreeIndexImpl(@NotNull final TreeIndexImpl treeIndexImpl) {
        return new CurrentOnlyIndex(treeIndexImpl);
    }

    @NotNull
    @Override
    protected Index.Factory getFactory() {
        return Index.CURRENT_FACTORY;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
