//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Checks that the auto-import documentation in
 * {@code docs/{python,groovy}/reference/query-language/query-library/auto-imported/} is in sync with the static methods
 * and constants declared in {@link QueryLibraryImportsDefaults#statics()}.
 * <p>
 * Only classes whose fully-qualified names match one of {@link #DOCUMENTED_CLASS_PREFIXES} are checked — mirroring the
 * {@code CATEGORY_FILTERS} in {@code generate_autoimport_docs.py}.
 * <p>
 * Run via: {@code ./gradlew :engine-table:checkAutoImportSync}
 */
public class CheckAutoImportDocSync {

    /** Matches FUNCTION or CONSTANT rows, capturing name (group 1) and sig link text (group 2). */
    private static final Pattern TABLE_ROW_PATTERN = Pattern
            .compile("^\\s*\\|\\s*(?:FUNCTION|CONSTANT)\\s*\\|\\s*(\\w+)\\s*\\|\\s*\\[((?:[^\\]]|\\](?!\\())*?)\\]\\(");

    /**
     * Class name prefixes that {@code generate_autoimport_docs.py} documents (mirrors {@code CATEGORY_FILTERS}).
     * Classes in {@link QueryLibraryImportsDefaults#statics()} whose names do not start with one of these prefixes are
     * intentionally undocumented and are excluded from this check.
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
            "io.deephaven.engine.util.ColorUtilImpl",
            "io.deephaven.base.string.cache.CompressedString",
            "io.deephaven.engine.table.impl.verify.TableAssertions");

    /**
     * Standard Java/enum boilerplate methods that are never documented even when their declaring class is in scope.
     */
    private static final Set<String> EXCLUDED_METHOD_NAMES = new HashSet<>(Arrays.asList(
            "values", "valueOf", "compareTo", "ordinal", "name", "getDeclaringClass"));

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CheckAutoImportDocSync <docs-dir>");
            System.exit(2);
        }

        Path docsDir = Paths.get(args[0]);

        // documentedKeys: only from DOCUMENTED_CLASS_PREFIXES — used to detect methods that are
        // in scope for the generator but missing from the docs.
        Set<String> documentedKeys = buildCodeKeys(true);
        // allStaticsKeys: from every class in statics() — used to detect doc entries that are
        // completely absent from the codebase (not just outside the documented set).
        Set<String> allStaticsKeys = buildCodeKeys(false);

        System.out.println("Documented-scope method/field keys: " + documentedKeys.size());
        System.out.println("All statics() method/field keys:    " + allStaticsKeys.size());

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

            Set<String> docKeys = buildDocKeys(autoImportedDir);
            System.out.println(lang + " docs unique method/field keys: " + docKeys.size());

            // Methods the generator would produce but the docs don't have
            Set<String> missing = new TreeSet<>(documentedKeys);
            missing.removeAll(docKeys);

            // Methods the docs reference that don't exist anywhere in statics()
            Set<String> phantom = new TreeSet<>(docKeys);
            phantom.removeAll(allStaticsKeys);

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
     * Builds {@code name|sig} keys from statics(). {@code filteredOnly=true} limits to
     * {@link #DOCUMENTED_CLASS_PREFIXES}; {@code false} includes all statics() classes.
     */
    private static Set<String> buildCodeKeys(boolean filteredOnly) {
        QueryLibraryImportsDefaults defaults = new QueryLibraryImportsDefaults();
        Set<String> keys = new TreeSet<>();
        for (Class<?> cls : defaults.statics()) {
            if (filteredOnly && !isDocumented(cls)) {
                continue;
            }
            for (Method m : cls.getMethods()) {
                if (Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers())
                        && !EXCLUDED_METHOD_NAMES.contains(m.getName())) {
                    keys.add(m.getName() + "|" + methodSig(m));
                }
            }
            for (Field f : cls.getFields()) {
                if (Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers())
                        && !EXCLUDED_METHOD_NAMES.contains(f.getName())) {
                    keys.add(f.getName() + "|" + f.getType().getSimpleName());
                }
            }
        }
        return keys;
    }

    /** Returns {@code returnType(argType, ...)} matching the generator's {@code sep_signature(method_signature())}. */
    private static String methodSig(Method m) {
        String rt = m.getReturnType().getSimpleName();
        String args = Arrays.stream(m.getParameterTypes())
                .map(Class::getSimpleName)
                .collect(Collectors.joining(", "));
        if (m.isVarArgs()) {
            int last = args.lastIndexOf("[]");
            if (last >= 0) {
                args = args.substring(0, last) + "..." + args.substring(last + 2);
            }
        }
        return rt + "(" + args + ")";
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

    /** Parses .md files in dir, collecting name|sig keys from FUNCTION/CONSTANT rows; unescapes Markdown. */
    private static Set<String> buildDocKeys(Path dir) throws IOException {
        Set<String> keys = new TreeSet<>();
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
                    String name = m.group(1);
                    String sig = m.group(2)
                            .replace("\\(", "(")
                            .replace("\\)", ")")
                            .replace("\\[", "[")
                            .replace("\\]", "]");
                    keys.add(name + "|" + sig);
                }
            }
        }
        return keys;
    }
}
