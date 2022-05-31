package io.deephaven.plot.util;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Collections;

/**
 * Repository of general utilities for plot code generation.
 */
public class PlotGeneratorUtils {

    private static final TIntObjectMap<String> cachedIndents = new TIntObjectHashMap<>();

    /**
     * Get a String of spaces for indenting code.
     *
     * @param n The number of indents
     * @return The String for indenting code with spaces
     */
    static String indent(final int n) {
        String cached = cachedIndents.get(n);
        if (cached == null) {
            cachedIndents.put(n, cached = String.join("", Collections.nCopies(4 * n, " ")));
        }
        return cached;
    }
}
