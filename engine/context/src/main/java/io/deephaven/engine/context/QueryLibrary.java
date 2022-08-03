/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class QueryLibrary {
    private static final QueryLibraryImports IMPORTS_INSTANCE = QueryLibraryImports.copyFromServiceLoader();

    public interface Context {
        void updateVersionString();

        Collection<String> getImportStrings();

        Collection<Package> getPackageImports();

        Collection<Class<?>> getClassImports();

        Collection<Class<?>> getStaticImports();

        void importPackage(Package aPackage);

        void importClass(Class<?> aClass);

        void importStatic(Class<?> aClass);
    }

    public static QueryLibrary.Context makeNewLibrary() {
        return new QueryLibrary.ContextImpl(IMPORTS_INSTANCE);
    }

    @VisibleForTesting
    public static QueryLibrary.Context makeNewLibrary(String libraryVersion) {
        final QueryLibrary.ContextImpl ql = (QueryLibrary.ContextImpl) makeNewLibrary();
        ql.versionString = libraryVersion;
        return ql;
    }

    public static void resetLibrary() {
        setLibrary(null);
    }

    public static void setLibrary(QueryLibrary.Context library) {
        ExecutionContext.setContext(ExecutionContext.newBuilder()
                .setQueryLibrary(library == null ? PoisonedQueryLibrary.INSTANCE : library)
                .captureMutableQueryScope()
                .captureCompilerContext()
                .markSystemic()
                .build());
    }

    public static QueryLibrary.Context getLibrary() {
        return ExecutionContext.getContext().getQueryLibrary();
    }

    public static void importPackage(Package aPackage) {
        getLibrary().importPackage(aPackage);
    }

    public static void importClass(Class<?> aClass) {
        getLibrary().importClass(aClass);
    }

    public static void importStatic(Class<?> aClass) {
        getLibrary().importStatic(aClass);
    }

    public static Collection<String> getImportStrings() {
        return getLibrary().getImportStrings();
    }

    public static Collection<Package> getPackageImports() {
        return getLibrary().getPackageImports();
    }

    public static Collection<Class<?>> getClassImports() {
        return getLibrary().getClassImports();
    }

    public static Collection<Class<?>> getStaticImports() {
        return getLibrary().getStaticImports();
    }

    private static class ContextImpl implements Context {
        // Any dynamically-added package, class, or static import may alter the meaning of the Java code
        // we are compiling. So when this happens, we dynamically generate a new globally-unique version string.
        private String versionString;

        private final Map<String, Package> packageImports;
        private final Map<String, Class<?>> classImports;
        private final Map<String, Class<?>> staticImports;

        private ContextImpl(QueryLibraryImports imports) {
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

        @Override
        public Collection<Class<?>> getClassImports() {
            return Collections.unmodifiableCollection(classImports.values());
        }

        @Override
        public Collection<Class<?>> getStaticImports() {
            return Collections.unmodifiableCollection(staticImports.values());
        }

        @Override
        public Collection<Package> getPackageImports() {
            return Collections.unmodifiableCollection(packageImports.values());
        }

        @Override
        public void importPackage(Package aPackage) {
            final Package previous = packageImports.put(aPackage.getName(), aPackage);
            if (aPackage != previous) {
                updateVersionString();
            }
        }

        @Override
        public void importClass(Class<?> aClass) {
            final Class<?> previous = classImports.put(aClass.getName(), aClass);
            if (aClass != previous) {
                updateVersionString();
            }
        }

        @Override
        public void importStatic(Class<?> aClass) {
            final Class<?> previous = staticImports.put(aClass.getCanonicalName(), aClass);
            if (aClass != previous) {
                updateVersionString();
            }
        }

        @Override
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
    }
}
