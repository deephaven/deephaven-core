package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.GroupingProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A base {@link GroupingProvider} that supports memoization.
 */
public abstract class MemoizingGroupingProvider implements GroupingProvider {
    private final Map<GroupingMemoKey, MemoizedGrouping> memoizedGroupings = new HashMap<>();

    private static class MemoizedGrouping {
        private volatile SoftReference<Table> reference;

        Table getOrCompute(Supplier<Table> computator) {
            Table cachedResult = getIfValid();
            if (cachedResult != null) {
                return cachedResult;
            }

            synchronized (this) {
                cachedResult = getIfValid();
                if (cachedResult != null) {
                    return cachedResult;
                }

                final Table result = computator.get();
                reference = new SoftReference<>(result);
                return result;
            }
        }

        Table getIfValid() {
            if (reference == null) {
                return null;
            }

            return reference.get();
        }
    }

    /**
     * Get the memoized grouping, or compute it if none existed.
     *
     * @param memoKey the memo key, or null if memoization is not allowed.
     * @param groupingFactory the method to create the grouping
     * @return the resultant grouping.
     */
    @Nullable
    protected Table memoizeGrouping(@Nullable GroupingMemoKey memoKey, @NotNull Supplier<Table> groupingFactory) {
        if (memoKey == null) {
            return groupingFactory.get();
        }

        final MemoizedGrouping maybeMemo;
        synchronized (this) {
            maybeMemo = memoizedGroupings.computeIfAbsent(memoKey, t -> new MemoizedGrouping());
        }
        return maybeMemo.getOrCompute(groupingFactory);
    }

    /**
     * Clear all memoized groupings.
     */
    public synchronized void clearMemoizedGroupings() {
        memoizedGroupings.clear();
    }
}
