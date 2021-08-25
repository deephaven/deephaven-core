package io.deephaven.db.tables.libs;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.util.ClassList;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class QueryLibraryImportsConfiguration implements QueryLibraryImports {

    @Override
    public Set<Package> packages() {
        try {
            return new HashSet<>(ClassList
                    .readPackageList(Configuration.getInstance().getProperty("QueryLibrary.defaultPackageImportList")));
        } catch (IOException e) {
            throw new RuntimeException("Can not load default class imports", e);
        }
    }

    @Override
    public Set<Class<?>> classes() {
        try {
            return new HashSet<>(ClassList.readClassListAsCollection(
                    Configuration.getInstance().getProperty("QueryLibrary.defaultClassImportList")));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Can not load default class imports", e);
        }
    }

    @Override
    public Set<Class<?>> statics() {
        try {
            return new HashSet<>(ClassList.readClassListAsCollection(
                    Configuration.getInstance().getProperty("QueryLibrary.defaultStaticImportList")));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Can not load default static imports", e);
        }
    }
}
