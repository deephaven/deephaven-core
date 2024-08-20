//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.caching;

import io.deephaven.base.cache.OpenAddressedCanonicalizationCache;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.configuration.Configuration;
import io.deephaven.tuple.ArrayTuple;
import org.jetbrains.annotations.NotNull;

public class C14nUtil {

    public static final boolean ENABLED = Configuration.getInstance().getBooleanWithDefault("C14nUtil.enabled", false);

    /**
     * An CanonicalizationCache instance that can/should be used by "general" utilities that want to store canonicalized
     * objects in memory. Shared by StringUtils.
     */
    public static final OpenAddressedCanonicalizationCache CACHE =
            ENABLED ? new OpenAddressedCanonicalizationCache(10000) : null;

    /**
     * A whitelist of classes that we'll canonicalize in the maybeCanonicalize* methods. Mutable classes or classes with
     * hashCode() and equals(...) definitions that violate the usual contracts are dangerous and unsupported.
     */
    private static final Class<?>[] ELIGIBLE_CLASSES = new Class[] {
            // Strings/CompressedStrings are the ideal classes for this functionality. In Java 8, though, we might
            // be able to use -XX:+UseStringDeduplication with G1, which is arguably better.
            String.class,
            CompressedString.class,

            // Instants used in aggregations are most likely expirations.
            // Instant.class,

            // If we're going to bother canonicalizing key members, we might as well do the keys themselves.
            // ArrayTuple.class

            // Primitive wrappers are appropriate to include, but I'm not sure the benefits outweigh the costs.
            // Uncomment if we find otherwise.
            // Boolean.class,
            // Character.class,
            // Byte.class,
            // Short.class,
            // Integer.class,
            // Long.class,
            // Float.class,
            // Double.class,
    };

    /**
     * @param clazz
     * @return true iff instances of clazz would be canonicalized by maybeCanonicalize*
     */
    private static boolean eligible(final Class<?> clazz) {
        for (int ci = 0; ci < ELIGIBLE_CLASSES.length; ++ci) {
            if (clazz == ELIGIBLE_CLASSES[ci]) {
                return true;
            }
        }
        return false;
    }

    /**
     * Canonicalize an object using the default CanonicalizationCache, with no type checking.
     * 
     * @param item
     * @param <T>
     * @return null if item was null, else the canonicalized version of item, which may be the same instance
     */
    private static <T> T canonicalize(final T item) {
        return item == null ? null : CACHE.getCachedItem(item);
    }

    /**
     * Canonicalize an object using the default CanonicalizationCache, if it's an instance of a known appropriate class.
     * 
     * @param item
     * @param <T>
     * @return null if item was null, else the canonicalized version of item if its class was eligible, else item
     */
    public static <T> T maybeCanonicalize(final T item) {
        return !ENABLED || item == null || !eligible(item.getClass()) ? item : CACHE.getCachedItem(item);
    }

    /**
     * Canonicalizes an array of objects in-place using the default CanonicalizationCache, with no type checking.
     * 
     * @param items
     * @param <T>
     * @return items
     */
    private static <T> T[] canonicalizeAll(@NotNull final T[] items) {
        for (int ii = 0; ii < items.length; ++ii) {
            items[ii] = canonicalize(items[ii]);
        }
        return items;
    }

    /**
     * Canonicalizes an array of objects in-place using the default CanonicalizationCache, if they're instances of known
     * appropriate classes. May canonicalize some items without canonicalizing all.
     * 
     * @param items
     * @return true if all non-null items were canonicalized, else false.
     */
    public static <T> boolean maybeCanonicalizeAll(@NotNull final T[] items) {
        if (!ENABLED) {
            return false;
        }
        boolean allCanonicalized = true;
        for (int ii = 0; ii < items.length; ++ii) {
            final T item = items[ii];
            if (item == null) {
                continue;
            }
            if (!eligible(item.getClass())) {
                allCanonicalized = false;
                continue;
            }
            items[ii] = CACHE.getCachedItem(item);
        }
        return allCanonicalized;
    }

    private static final ArrayTuple COMPOUND_KEY_SINGLE_NULL = new ArrayTuple((Object) null);

    /**
     * Return a key that represents the {@code values}, canonicalizing all values and the result itself if
     * canonicalization is enabled.
     * 
     * @param values the value or values to turn into a key
     * @return an opaque, potentially-canonicalized key for use in a map
     */
    public static Object maybeMakeCompoundKey(final Object... values) {
        // In the long run, we can do better than ArrayTuple
        if (values.length == 1) {
            return values[0] == null ? COMPOUND_KEY_SINGLE_NULL : maybeCanonicalize(values[0]);
        } else {
            maybeCanonicalizeAll(values);
            return maybeCanonicalize(new ArrayTuple(values));
        }
    }
}
