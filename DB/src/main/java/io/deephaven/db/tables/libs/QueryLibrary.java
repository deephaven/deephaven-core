/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.libs;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.db.v2.utils.codegen.CodeGenerator;
import groovy.lang.GroovyClassLoader;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class QueryLibrary {

    private static final QueryLibraryImports IMPORTS_INSTANCE = QueryLibraryImports.copyFromServiceLoader();

    private final Map<String, Package> packageImports;
    private final Map<String, Class<?>> classImports;
    private final Map<String, Class<?>> staticImports;
    private String versionString;

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

    private static volatile QueryLibrary defaultLibrary = null;
    private final static ThreadLocal<QueryLibrary> currLibrary =
            ThreadLocal.withInitial(QueryLibrary::getDefaultLibrary);

    private static QueryLibrary getDefaultLibrary() {
        if (defaultLibrary == null) {
            synchronized (QueryLibrary.class) {
                if (defaultLibrary == null) {
                    defaultLibrary = makeNewLibrary();
                }
            }
        }
        return defaultLibrary;
    }

    /**
     * Sets the default library.
     *
     * @param library the script session's query library
     * @throws IllegalStateException if default library is already set
     * @throws NullPointerException if library is null
     */
    public static synchronized void setDefaultLibrary(final QueryLibrary library) {
        if (defaultLibrary != null) {
            throw new IllegalStateException(
                    "It's too late to set default library; it's already set to: " + defaultLibrary);
        }
        defaultLibrary = Objects.requireNonNull(library);
    }

    public void updateVersionString() {
        versionString = UuidCreator.toString(UuidCreator.getRandomBased());
    }

    public static QueryLibrary makeNewLibrary() {
        return new QueryLibrary(IMPORTS_INSTANCE);
    }

    @VisibleForTesting
    public static QueryLibrary makeNewLibrary(String libraryVersion) {
        final QueryLibrary ql = new QueryLibrary(IMPORTS_INSTANCE);
        ql.versionString = libraryVersion;
        return ql;
    }

    public static void resetLibrary() {
        setLibrary(makeNewLibrary());
    }

    public static void setLibrary(QueryLibrary library) {
        currLibrary.set(library);
    }

    public static QueryLibrary getLibrary() {
        return currLibrary.get();
    }

    public static void importPackage(Package aPackage) {
        // Any dynamically-added package, class, or static import may alter the meaning of the Java code
        // we are compiling. So when this happens, we dynamically generate a new globally-unique version string.
        final QueryLibrary lql = currLibrary.get();
        final Package previous = lql.packageImports.put(aPackage.getName(), aPackage);
        if (aPackage != previous) {
            lql.updateVersionString();
        }
    }

    public static void importClass(Class aClass) {
        // Any dynamically-added package, class, or static import may alter the meaning of the Java code
        // we are compiling. So when this happens, we dynamically generate a new globally-unique version string.
        final QueryLibrary lql = currLibrary.get();
        final Class previous = lql.classImports.put(aClass.getCanonicalName(), aClass);
        if (aClass.getClassLoader() instanceof GroovyClassLoader) {
            if (aClass != previous) {
                lql.updateVersionString();
            }
        }
    }

    public static void importStatic(Class aClass) {
        // Any dynamically-added package, class, or static import may alter the meaning of the Java code
        // we are compiling. So when this happens, we dynamically generate a new globally-unique version string.
        final QueryLibrary lql = currLibrary.get();
        final Class previous = lql.staticImports.put(aClass.getCanonicalName(), aClass);
        if (aClass.getClassLoader() instanceof GroovyClassLoader) {
            if (aClass != previous) {
                lql.updateVersionString();
            }
        }
    }

    public static CodeGenerator getImportStatement() {
        final List<String> imports = new ArrayList<>();
        final QueryLibrary lql = currLibrary.get();
        imports.add("// QueryLibrary internal version number: " + lql.versionString);
        for (final Package packageImport : lql.packageImports.values()) {
            imports.add("import " + packageImport.getName() + ".*;");
        }
        for (final Class<?> classImport : lql.classImports.values()) {
            if (classImport.getDeclaringClass() != null) {
                imports.add("import static " + classImport.getCanonicalName() + ";");
            } else if (!lql.packageImports.containsKey(classImport.getPackage().getName())) {
                imports.add("import " + classImport.getName() + ";");
            }
        }
        for (final Class<?> staticImport : lql.staticImports.values()) {
            imports.add("import static " + staticImport.getCanonicalName() + ".*;");
        }
        return CodeGenerator.create(imports.toArray());
    }

    public static Collection<Package> getPackageImports() {
        return Collections.unmodifiableCollection(currLibrary.get().packageImports.values());
    }

    public static Collection<Class<?>> getClassImports() {
        return Collections.unmodifiableCollection(currLibrary.get().classImports.values());
    }

    public static Collection<Class<?>> getStaticImports() {
        return Collections.unmodifiableCollection(currLibrary.get().staticImports.values());
    }
}
