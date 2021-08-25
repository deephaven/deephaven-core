package io.deephaven.db.tables.libs;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public interface QueryLibraryImports {
    static QueryLibraryImports copyFromServiceLoader() {
        return copyFrom(new QueryLibraryImportsServiceLoader());
    }

    static QueryLibraryImports copyFrom(QueryLibraryImports other) {
        final Set<Package> packages = Collections.unmodifiableSet(new LinkedHashSet<>(other.packages()));
        final Set<Class<?>> classes = Collections.unmodifiableSet(new LinkedHashSet<>(other.classes()));
        final Set<Class<?>> statics = Collections.unmodifiableSet(new LinkedHashSet<>(other.statics()));
        return new QueryLibraryImportsImpl(packages, classes, statics);
    }

    Set<Package> packages();

    Set<Class<?>> classes();

    Set<Class<?>> statics();
}
