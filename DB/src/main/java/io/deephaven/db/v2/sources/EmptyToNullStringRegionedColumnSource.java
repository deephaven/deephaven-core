package io.deephaven.db.v2.sources;

import io.deephaven.base.Pair;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.base.string.cache.StringCacheTypeAdapterCompressedStringImpl;
import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.GroupingProvider;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Regioned ColumnSource that replaces empty CharSequences with null.
 */
public class EmptyToNullStringRegionedColumnSource<STRING_LIKE_TYPE extends CharSequence> extends AbstractColumnSource<STRING_LIKE_TYPE> implements MutableColumnSourceGetDefaults.ForObject<STRING_LIKE_TYPE> {

    private final DeferredGroupingColumnSource<STRING_LIKE_TYPE> innerSource;

    private Runnable callback;

    /**
     * Wrap inner source with a column source that replaces empty values with null.
     * Callback will be invoked on the first occasion for remapping (modulo race conditions which may permit
     * it to be invoked more than once).
     * @param innerSource The inner source
     * @param callback The callback
     */
    public EmptyToNullStringRegionedColumnSource(@NotNull final DeferredGroupingColumnSource<STRING_LIKE_TYPE> innerSource,
                                                 @Nullable final Runnable callback) {
        super(innerSource.getType());
        this.innerSource = Require.neqNull(innerSource, "innerSource");
        this.callback = callback;
    }

    private STRING_LIKE_TYPE mapEmptyToNull(final STRING_LIKE_TYPE innerValue) {
        if (innerValue == null || innerValue.length() != 0) {
            return innerValue;
        }
        // If we invoke the callback on more than one thread before clearing is visible, so be it.
        if (callback != null) {
            callback.run();
            callback = null;
        }
        return null;
    }

    @Override
    public STRING_LIKE_TYPE get(final long index) {
        return mapEmptyToNull(innerSource.get(index));
    }

    @Override
    public STRING_LIKE_TYPE getPrev(final long index) {
        return mapEmptyToNull(innerSource.getPrev(index));
    }

    @Override
    public boolean isImmutable() {
        return innerSource.isImmutable();
    }

    @Override
    public void releaseCachedResources() {
        innerSource.releaseCachedResources();
    }

    private Map<STRING_LIKE_TYPE, Index> combineEmptyAndNullGroups(@Nullable Map<STRING_LIKE_TYPE, Index> innerGroups) {
        if (innerGroups == null) {
            return null;
        }

        final STRING_LIKE_TYPE emptyValue;
        if (getType() == String.class) {
            //noinspection unchecked
            emptyValue = (STRING_LIKE_TYPE) "";
        } else if (getType() == CompressedString.class) {
            //noinspection unchecked
            emptyValue = (STRING_LIKE_TYPE) StringCacheTypeAdapterCompressedStringImpl.INSTANCE.empty();
        } else {
            // Should have blown up long ago.
            throw new UnsupportedOperationException("Unsupported CharSequence type " + getType());
        }

        final Index indexForEmpty = innerGroups.remove(emptyValue);
        if (indexForEmpty == null || indexForEmpty.empty()) {
            return innerGroups;
        }

        final Index indexForNull = innerGroups.get(null);
        if (indexForNull == null || indexForNull.empty()) {
            innerGroups.put(null, indexForEmpty);
        } else {
            indexForNull.insert(indexForEmpty);
        }
        return innerGroups;
    }

    @Override
    public final Map<STRING_LIKE_TYPE, Index> getGroupToRange() {
        if (groupToRange == null && innerSource.getGroupingProvider() != null) {
            groupToRange = combineEmptyAndNullGroups(innerSource.getGroupToRange());
            innerSource.setGroupingProvider(null);
        }
        return groupToRange;
    }

    @Override
    public final Map<STRING_LIKE_TYPE, Index> getGroupToRange(Index index) {
        final GroupingProvider<STRING_LIKE_TYPE> innerGroupingProvider = innerSource.getGroupingProvider();
        if (groupToRange == null && innerGroupingProvider != null) {
            final Pair<Map<STRING_LIKE_TYPE, Index>, Boolean> result = innerGroupingProvider.getGroupToRange(index);
            if (result == null) {
                return null;
            }
            if (result.second) {
                groupToRange = combineEmptyAndNullGroups(result.first);
                innerSource.setGroupingProvider(null);
            }
            return combineEmptyAndNullGroups(result.first);
        }
        return groupToRange;
    }
}
