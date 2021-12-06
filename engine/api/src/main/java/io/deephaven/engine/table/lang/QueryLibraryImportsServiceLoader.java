package io.deephaven.engine.table.lang;

import java.util.LinkedHashSet;
import java.util.ServiceLoader;
import java.util.Set;

public class QueryLibraryImportsServiceLoader implements QueryLibraryImports {

    @Override
    public Set<Package> packages() {
        Set<Package> packages = new LinkedHashSet<>();
        for (QueryLibraryImports qli : ServiceLoader.load(QueryLibraryImports.class)) {
            packages.addAll(qli.packages());
        }
        return packages;
    }

    @Override
    public Set<Class<?>> classes() {
        Set<Class<?>> classes = new LinkedHashSet<>();
        for (QueryLibraryImports qli : ServiceLoader.load(QueryLibraryImports.class)) {
            classes.addAll(qli.classes());
        }
        return classes;
    }

    @Override
    public Set<Class<?>> statics() {
        Set<Class<?>> statics = new LinkedHashSet<>();
        for (QueryLibraryImports qli : ServiceLoader.load(QueryLibraryImports.class)) {
            statics.addAll(qli.statics());
        }
        return statics;
    }
}
