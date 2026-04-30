package io.deephaven.engine.table.lang.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Checks that the auto-import documentation in {@code docs/{python,groovy}/reference/query-language/query-library/auto-imported/}
 * is in sync with the static methods and constants declared in
 * {@link QueryLibraryImportsDefaults#statics()}.
 * <p>
 * Only classes whose fully-qualified names match one of {@link #DOCUMENTED_CLASS_PREFIXES} are
 * checked — mirroring the {@code CATEGORY_FILTERS} in {@code generate_autoimport_docs.py}.
 * <p>
 * Run via: {@code ./gradlew :engine-table:checkAutoImportSync}
 */
public class CheckAutoImportDocSync {

    /** Matches FUNCTION or CONSTANT rows in the auto-imported markdown tables. */
    private static final Pattern TABLE_ROW_PATTERN =
            Pattern.compile("^\\s*\\|\\s*(FUNCTION|CONSTANT)\\s*\\|\\s*(\\w+)\\s*\\|");

    /**
     * Class name prefixes that {@code generate_autoimport_docs.py} documents (mirrors
     * {@code CATEGORY_FILTERS}). Classes in {@link QueryLibraryImportsDefaults#statics()} whose
     * names do not start with one of these prefixes are intentionally undocumented and are
     * excluded from this check.
     */
    private static final List<String> DOCUMENTED_CLASS_PREFIXES = Arrays.asList(
            "io.deephaven.util.QueryConstants",
            "io.deephaven.function.Basic",
            "io.deephaven.function.BinSearch",
            "io.deephaven.function.Cast",
            "io.deephaven.function.Logic",
            "io.deephaven.function.Numeric",
            "io.deephaven.function.Parse",
            "io.deephaven.function.Random",
            "io.deephaven.function.Sort",
            "io.deephaven.time.",
            "io.deephaven.gui.",
            "io.deephaven.engine.util.ColorUtilImpl");

    /**
     * Standard Java/enum boilerplate methods that are never documented even when their declaring
     * class is in scope.
     */
    private static final Set<String> EXCLUDED_METHOD_NAMES = new HashSet<>(Arrays.asList(
            "values", "valueOf", "compareTo", "ordinal", "name", "getDeclaringClass"));

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CheckAutoImportDocSync <docs-dir>");
            System.exit(2);
        }

        Path docsDir = Paths.get(args[0]);

        // documentedNames: only from DOCUMENTED_CLASS_PREFIXES — used to detect methods that are
        // in scope for the generator but missing from the docs.
        Set<String> documentedNames = buildCodeNames(true);
        // allStaticsNames: from every class in statics() — used to detect doc entries that are
        // completely absent from the codebase (not just outside the documented set).
        Set<String> allStaticsNames = buildCodeNames(false);

        System.out.println("Documented-scope method/field names: " + documentedNames.size());
        System.out.println("All statics() method/field names:    " + allStaticsNames.size());

        boolean failed = false;
        for (String lang : new String[] {"python", "groovy"}) {
            Path autoImportedDir = docsDir
                    .resolve(lang)
                    .resolve("reference/query-language/query-library/auto-imported");

            if (!Files.isDirectory(autoImportedDir)) {
                System.err.println("ERROR: directory not found: " + autoImportedDir);
                failed = true;
                continue;
            }

            Set<String> docNames = buildDocNames(autoImportedDir);
            System.out.println(lang + " docs unique method/field names: " + docNames.size());

            // Methods the generator would produce but the docs don't have
            Set<String> missing = new TreeSet<>(documentedNames);
            missing.removeAll(docNames);

            // Methods the docs reference that don't exist anywhere in statics()
            Set<String> phantom = new TreeSet<>(docNames);
            phantom.removeAll(allStaticsNames);

            if (missing.isEmpty() && phantom.isEmpty()) {
                System.out.println("=== " + lang + ": in sync ===");
            } else {
                System.err.println("=== " + lang + ": OUT OF SYNC ===");
                if (!missing.isEmpty()) {
                    System.err.println("  In code but missing from docs (" + missing.size() + "): "
                            + missing);
                }
                if (!phantom.isEmpty()) {
                    System.err.println(
                            "  In docs but absent from all statics() (" + phantom.size() + "): "
                                    + phantom);
                }
                failed = true;
            }
        }

        if (failed) {
            System.err.println(
                    "\nAuto-import docs are out of sync with QueryLibraryImportsDefaults."
                            + " See docs/tools/autoimport/README.md to regenerate.");
            System.exit(1);
        }
    }

    /**
     * Reflects on every class in {@link QueryLibraryImportsDefaults#statics()} and collects the
     * names of all public static methods and public static fields, excluding
     * {@link #EXCLUDED_METHOD_NAMES}.
     *
     * @param filteredOnly if {@code true}, only includes classes matching
     *        {@link #DOCUMENTED_CLASS_PREFIXES}; if {@code false}, includes all statics() classes
     */
    private static Set<String> buildCodeNames(boolean filteredOnly) {
        QueryLibraryImportsDefaults defaults = new QueryLibraryImportsDefaults();
        Set<String> names = new TreeSet<>();
        for (Class<?> cls : defaults.statics()) {
            if (filteredOnly && !isDocumented(cls)) {
                continue;
            }
            for (Method m : cls.getDeclaredMethods()) {
                if (Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers())
                        && !EXCLUDED_METHOD_NAMES.contains(m.getName())) {
                    names.add(m.getName());
                }
            }
            for (Field f : cls.getDeclaredFields()) {
                if (Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers())
                        && !EXCLUDED_METHOD_NAMES.contains(f.getName())) {
                    names.add(f.getName());
                }
            }
        }
        return names;
    }

    private static boolean isDocumented(Class<?> cls) {
        String name = cls.getName();
        for (String prefix : DOCUMENTED_CLASS_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parses every {@code .md} file in {@code dir} (excluding {@code index.md}) and collects the
     * names found in FUNCTION/CONSTANT table rows.
     */
    private static Set<String> buildDocNames(Path dir) throws IOException {
        Set<String> names = new TreeSet<>();
        List<Path> files = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(p -> {
                String name = p.getFileName().toString();
                return name.endsWith(".md") && !name.equals("index.md");
            }).forEach(files::add);
        }
        for (Path file : files) {
            for (String line : Files.readAllLines(file)) {
                Matcher m = TABLE_ROW_PATTERN.matcher(line);
                if (m.find()) {
                    names.add(m.group(2));
                }
            }
        }
        return names;
    }
}
