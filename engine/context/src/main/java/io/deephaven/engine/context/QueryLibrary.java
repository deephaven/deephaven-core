//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import com.github.f4b6a3.uuid.UuidCreator;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class QueryLibrary {
    private static final QueryLibraryImports IMPORTS_INSTANCE = QueryLibraryImports.copyFromServiceLoader();

    static QueryLibrary makeNewLibrary() {
        return new QueryLibrary(IMPORTS_INSTANCE);
    }

    static QueryLibrary makeNewLibrary(String libraryVersion) {
        final QueryLibrary ql = makeNewLibrary();
        ql.versionString = libraryVersion;
        return ql;
    }

    // Any dynamically-added package, class, or static import may alter the meaning of the Java code
    // we are compiling. So when this happens, we dynamically generate a new globally-unique version string.
    private String versionString;

    private final Map<String, Package> packageImports;
    private final Set<String> classImports;
    private final Set<String> staticImports;

    /** package-private constructor for {@link io.deephaven.engine.context.PoisonedQueryLibrary} */
    QueryLibrary() {
        packageImports = null;
        classImports = null;
        staticImports = null;
    }

    private QueryLibrary(QueryLibraryImports imports) {
        packageImports = new ConcurrentSkipListMap<>();
        for (Package p : imports.packages()) {
            packageImports.put(p.getName(), p);
        }
        classImports = new ConcurrentSkipListSet<>();
        for (Class<?> c : imports.classes()) {
            classImports.add(c.getName());
        }
        staticImports = new ConcurrentSkipListSet<>();
        for (Class<?> c : imports.statics()) {
            staticImports.add(c.getName());
        }
        updateVersionString();
    }

    private Class<?> load(String className) {
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class " + className, e);
        }
    }

    @Deprecated
    public Collection<Class<?>> getClassImports() {
        return classImports.stream()
                .map(this::load)
                .collect(Collectors.toUnmodifiableList());
    }

    @Deprecated
    public Collection<Class<?>> getStaticImports() {
        return staticImports.stream()
                .map(this::load)
                .collect(Collectors.toUnmodifiableList());
    }

    @Deprecated
    public Collection<Package> getPackageImports() {
        return Collections.unmodifiableCollection(packageImports.values());
    }

    public void importPackage(Package aPackage) {
        final Package previous = packageImports.put(aPackage.getName(), aPackage);
        if (aPackage != previous) {
            updateVersionString();
        }
    }

    public void importClass(Class<?> aClass) {
        if (classImports.add(aClass.getCanonicalName())) {
            updateVersionString();
        } else {
            //TODO consider weakrefs in a map, so we can see if the class is still loaded and avoid hitting this
            updateVersionString();
        }
    }

    public void importStatic(Class<?> aClass) {
        if (staticImports.add(aClass.getCanonicalName())) {
            updateVersionString();
        } else {
            //TODO consider weakrefs in a map, so we can see if the class is still loaded and avoid hitting this
            updateVersionString();
        }
    }

    /**
     * Builds a collection of imports and loads (but doesn't initialize) each class to ensure they are on the classpath.
     */
    public Collection<String> getImportStrings() {
        final List<String> imports = new ArrayList<>();

        imports.add("// QueryLibrary internal version number: " + versionString);
        for (final Package packageImport : packageImports.values()) {
            imports.add("import " + packageImport.getName() + ".*;");
        }
        for (final String classImport : classImports) {
            Class<?> c = load(classImport);
            if (c.getDeclaringClass() != null) {
                imports.add("import static " + classImport + ";");
            } else if (!packageImports.containsKey(classImport.substring(0, classImport.lastIndexOf('.')))) {
                imports.add("import " + classImport + ";");
            }
        }
        for (final String staticImport : staticImports) {
            load(staticImport);
            imports.add("import static " + staticImport + ".*;");
        }
        return imports;
    }

    public void updateVersionString() {
        versionString = UuidCreator.toString(UuidCreator.getRandomBased());
    }

    public void updateVersionString(String version) {
        versionString = version;
    }
}
