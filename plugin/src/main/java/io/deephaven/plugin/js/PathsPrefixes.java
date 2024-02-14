/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;
import java.util.Set;

@Immutable
@BuildableStyle
abstract class PathsPrefixes implements PathsInternal {

    public static Builder builder() {
        return ImmutablePathsPrefixes.builder();
    }

    public abstract Set<Path> prefixes();

    @Override
    public final boolean matches(Path path) {
        if (prefixes().contains(path)) {
            return true;
        }
        // Note: we could make a more efficient impl w/ a tree-based approach based on the names
        for (Path prefix : prefixes()) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    @Check
    final void checkPrefixesNonEmpty() {
        if (prefixes().isEmpty()) {
            throw new IllegalArgumentException("prefixes must be non-empty");
        }
    }

    interface Builder {
        Builder addPrefixes(Path element);

        Builder addPrefixes(Path... elements);

        Builder addAllPrefixes(Iterable<? extends Path> elements);

        PathsPrefixes build();
    }
}
