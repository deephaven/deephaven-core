//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.js;

import java.nio.file.Path;

/**
 * The subset of paths to serve, see {@link JsPlugin#paths()}.
 */
public interface Paths {

    /**
     * Includes all paths.
     *
     * @return the paths
     */
    static Paths all() {
        return PathsAll.ALL;
    }

    /**
     * Includes only the paths that are prefixed by {@code prefix}.
     *
     * @param prefix the prefix
     * @return the paths
     */
    static Paths ofPrefixes(Path prefix) {
        // Note: we have specific overload for single element to explicitly differentiate from Iterable overload since
        // Path extends Iterable<Path>.
        return PathsPrefixes.builder().addPrefixes(prefix).build();
    }

    /**
     * Includes only the paths that are prefixed by one of {@code prefixes}.
     *
     * @param prefixes the prefixes
     * @return the paths
     */
    static Paths ofPrefixes(Path... prefixes) {
        return PathsPrefixes.builder().addPrefixes(prefixes).build();
    }

    /**
     * Includes only the paths that are prefixed by one of {@code prefixes}.
     *
     * @param prefixes the prefixes
     * @return the paths
     */
    static Paths ofPrefixes(Iterable<? extends Path> prefixes) {
        return PathsPrefixes.builder().addAllPrefixes(prefixes).build();
    }
}
