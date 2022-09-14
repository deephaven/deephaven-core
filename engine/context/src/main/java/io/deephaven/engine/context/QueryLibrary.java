/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import com.github.f4b6a3.uuid.UuidCreator;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

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
    private final Map<String, Class<?>> classImports;
    private final Map<String, Class<?>> staticImports;

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
        classImports = new ConcurrentSkipListMap<>();
        for (Class<?> c : imports.classes()) {
            classImports.put(c.getCanonicalName(), c);
        }
        staticImports = new ConcurrentSkipListMap<>();
        for (Class<?> c : imports.statics()) {
            staticImports.put(c.getCanonicalName(), c);
        }
        updateVersionString();
    }

    public Collection<Class<?>> getClassImports() {
        return Collections.unmodifiableCollection(classImports.values());
    }

    public Collection<Class<?>> getStaticImports() {
        return Collections.unmodifiableCollection(staticImports.values());
    }

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
        final Class<?> previous = classImports.put(aClass.getName(), aClass);
        if (aClass != previous) {
            updateVersionString();
        }
    }

    public void importStatic(Class<?> aClass) {
        final Class<?> previous = staticImports.put(aClass.getCanonicalName(), aClass);
        if (aClass != previous) {
            updateVersionString();
        }
    }

    public Collection<String> getImportStrings() {
        final List<String> imports = new ArrayList<>();

        imports.add("// QueryLibrary internal version number: " + versionString);
        for (final Package packageImport : packageImports.values()) {
            imports.add("import " + packageImport.getName() + ".*;");
        }
        for (final Class<?> classImport : classImports.values()) {
            if (classImport.getDeclaringClass() != null) {
                imports.add("import static " + classImport.getCanonicalName() + ";");
            } else if (!packageImports.containsKey(classImport.getPackage().getName())) {
                imports.add("import " + classImport.getName() + ";");
            }
        }
        for (final Class<?> staticImport : staticImports.values()) {
            imports.add("import static " + staticImport.getCanonicalName() + ".*;");
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
