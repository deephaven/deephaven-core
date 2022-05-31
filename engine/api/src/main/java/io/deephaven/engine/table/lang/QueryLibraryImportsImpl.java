package io.deephaven.engine.table.lang;

import java.util.Objects;
import java.util.Set;

class QueryLibraryImportsImpl implements QueryLibraryImports {

    private final Set<Package> packages;
    private final Set<Class<?>> classes;
    private final Set<Class<?>> statics;

    QueryLibraryImportsImpl(
            Set<Package> packages, Set<Class<?>> classes, Set<Class<?>> statics) {
        this.packages = Objects.requireNonNull(packages);
        this.classes = Objects.requireNonNull(classes);
        this.statics = Objects.requireNonNull(statics);
    }

    @Override
    public Set<Package> packages() {
        return packages;
    }

    @Override
    public Set<Class<?>> classes() {
        return classes;
    }

    @Override
    public Set<Class<?>> statics() {
        return statics;
    }
}
