//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replication;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReplicationUtils {
    /**
     * Take a list of lines; and apply a given fixup expressed as a code region, regular expression, then function from
     * the matcher to the replacement line.
     *
     * @param lines the input lines
     * @param region the name of the region started by "// region &lt;name&gt;" and ended by "// endregion &lt;name&gt;"
     * @param searchPattern the pattern to search for
     * @param replacer a function from the search pattern's successful matcher to the new lines to apply as a List.
     * @return a new list of lines with the fixup applied
     */
    @NotNull
    public static List<String> applyFixup(List<String> lines, final String region, final String searchPattern,
            final Function<Matcher, List<String>> replacer) {
        final List<String> newLines = new ArrayList<>();

        final Pattern startPattern = constructRegionStartPattern(region);
        final Pattern endPattern = constructRegionEndPattern(region);

        final Pattern replacePattern = Pattern.compile(searchPattern);

        boolean inRegion = false;
        for (String line : lines) {
            if (startPattern.matcher(line).find()) {
                if (inRegion) {
                    throw new IllegalStateException();
                }
                inRegion = true;
            }
            if (endPattern.matcher(line).find()) {
                if (!inRegion) {
                    throw new IllegalStateException();
                }
                inRegion = false;
            }
            if (!inRegion) {
                newLines.add(line);
            } else {
                final Matcher matcher = replacePattern.matcher(line);
                final boolean matches = matcher.matches();
                if (matches) {
                    newLines.addAll(replacer.apply(matcher));
                } else {
                    newLines.add(line);
                }
            }
        }

        if (inRegion) {
            throw new IllegalStateException("Region " + region + " never ended!");
        }

        return newLines;
    }

    /**
     * Take a list of lines; and apply a given fixup expressed as a code region and replacements
     *
     * @param lines the input lines
     * @param region the name of the region started by "// region &lt;name&gt;" and ended by "// endregion &lt;name&gt;"
     * @param replacements an array with an even number of elements, even elements are a thing to replace, the next
     *        element is the thing to replace it with
     * @return a new list of lines with the fixup applied
     */
    @NotNull
    public static List<String> simpleFixup(List<String> lines, final String region, final String... replacements) {
        final List<String> newLines = new ArrayList<>();

        final Pattern startPattern = constructRegionStartPattern(region);
        final Pattern endPattern = constructRegionEndPattern(region);

        boolean inRegion = false;
        for (String line : lines) {
            if (startPattern.matcher(line).find()) {
                if (inRegion) {
                    throw new IllegalStateException();
                }
                inRegion = true;
            }
            if (endPattern.matcher(line).find()) {
                if (!inRegion) {
                    throw new IllegalStateException();
                }
                inRegion = false;
            }
            if (!inRegion) {
                newLines.add(line);
            } else {
                newLines.add(doLineReplacements(line, replacements));
            }
        }

        if (inRegion) {
            throw new IllegalStateException("Region " + region + " never ended!");
        }

        return newLines;
    }

    /**
     * Do whatever miscellaneous cleanups might be appropriate for all replicated classes. For now, this removes
     * identical import lines.
     */
    public static List<String> standardCleanups(List<String> lines) {
        final List<String> newLines = new ArrayList<>();
        final Set<String> imports = new HashSet<>();

        for (final String line : lines) {
            // Gets copied over if it is not an import statement, or if it is an import statement that we have seen
            // for the first time. Otherwise gets dropped.
            if (!line.startsWith("import") || imports.add(line)) {
                newLines.add(line);
            }
        }
        return newLines;
    }

    /**
     * Locates the region demarked by "// region &lt;name&gt;" and ended by "// endregion &lt;name&gt;" and adds extra
     * lines at the top.
     *
     * @param lines the lines to process
     * @param region the name of the region
     * @param extraLines the lines to insert
     * @return a new list of lines
     */
    @NotNull
    public static List<String> insertRegion(List<String> lines, final String region, List<String> extraLines) {
        final List<String> newLines = new ArrayList<>();

        final Pattern startPattern = constructRegionStartPattern(region);
        final Pattern endPattern = constructRegionEndPattern(region);

        boolean inRegion = false;
        for (String line : lines) {
            if (startPattern.matcher(line).find()) {
                if (inRegion) {
                    throw new IllegalStateException();
                }
                newLines.add(line);
                newLines.addAll(extraLines);
                inRegion = true;
                continue;
            }
            if (endPattern.matcher(line).find()) {
                if (!inRegion) {
                    throw new IllegalStateException();
                }
                inRegion = false;
            }
            newLines.add(line);
        }

        if (inRegion) {
            throw new IllegalStateException("Region " + region + " never ended!");
        }

        return newLines;
    }

    /**
     * Locates the region demarked by "// region &lt;name&gt;" and ended by "// endregion &lt;name&gt;" and removes it.
     *
     * @param lines the lines to process
     * @param region the name of the region
     * @return a new list of lines
     */
    @NotNull
    public static List<String> removeRegion(List<String> lines, final String region) {
        return replaceRegion(lines, region, Collections.emptyList());
    }

    /**
     * Locates the region demarked by "// region &lt;name&gt;" and ended by "// endregion &lt;name&gt;" and replaces the
     * text with the contents of replacement.
     *
     * @param lines the lines to process
     * @param region the name of the region
     * @param replacement the lines to insert
     * @return a new list of lines
     */
    @NotNull
    public static List<String> replaceRegion(List<String> lines, final String region, List<String> replacement) {
        return replaceRegion(lines, region, l -> replacement);
    }

    /**
     * Locates the region demarked by "// region &lt;name&gt;" and ended by "// endregion &lt;name&gt;" and replaces the
     * text with the contents of replacement.
     *
     * @param lines the lines to process
     * @param region the name of the region
     * @param replacement the lines to insert
     * @return a new list of lines
     */
    @NotNull
    public static List<String> replaceRegion(
            List<String> lines,
            final String region,
            Function<List<String>, List<String>> replacement) {
        final List<String> newLines = new ArrayList<>();

        final Pattern startPattern = constructRegionStartPattern(region);
        final Pattern endPattern = constructRegionEndPattern(region);

        final List<String> currentRegion = new ArrayList<>();
        boolean inRegion = false;
        for (String line : lines) {
            if (startPattern.matcher(line).find()) {
                if (inRegion) {
                    throw new IllegalStateException();
                }
                newLines.add(line);
                inRegion = true;
            } else if (endPattern.matcher(line).find()) {
                if (!inRegion) {
                    throw new IllegalStateException();
                }
                inRegion = false;
                newLines.addAll(replacement.apply(currentRegion));
                newLines.add(line);
            } else if (!inRegion) {
                newLines.add(line);
            } else {
                currentRegion.add(line);
            }
        }

        if (inRegion) {
            throw new IllegalStateException("Region " + region + " never ended!");
        }

        return newLines;
    }

    @NotNull
    private static Pattern constructRegionStartPattern(String region) {
        return Pattern.compile("//\\s*region " + region + "(?=\\s|$)");
    }

    @NotNull
    private static Pattern constructRegionEndPattern(String region) {
        return Pattern.compile("//\\s*endregion " + region + "(?=\\s|$)");
    }

    public static List<String> globalReplacements(List<String> lines, String... replacements) {
        if (replacements.length == 0 || replacements.length % 2 != 0) {
            throw new IllegalArgumentException("Bad replacement length: " + replacements.length);
        }
        return lines.stream().map(x -> doLineReplacements(x, replacements))
                .collect(Collectors.toList());
    }

    public static List<String> addImport(List<String> lines, Class... importClasses) {
        return addImport(lines,
                Arrays.stream(importClasses).map(c -> "import " + c.getCanonicalName() + ";").toArray(String[]::new));
    }

    public static List<String> removeImport(List<String> lines, Class... importClasses) {
        return removeImport(lines, Arrays.stream(importClasses)
                .map(c -> "\\s*import\\s+" + c.getCanonicalName() + "\\s*;").toArray(String[]::new));
    }

    public static List<String> addImport(List<String> lines, String... importString) {
        final List<String> newLines = new ArrayList<>(lines);
        for (int ii = 0; ii < newLines.size(); ++ii) {
            if (newLines.get(ii).matches("^package .*;")) {
                newLines.add(ii + 1, "");
                newLines.addAll(ii + 2, Arrays.asList(importString));
                return newLines;
            }
        }
        throw new IllegalArgumentException("Could not find package string!");
    }

    /**
     * Remove all of the specified imports -- Error if any are not found.
     */
    public static List<String> removeImport(List<String> lines, String... importRegex) {
        final List<Pattern> patterns = Arrays.stream(importRegex).map(Pattern::compile).collect(Collectors.toList());
        final List<String> newLines = removeAnyImports(lines, patterns);
        if (!patterns.isEmpty()) {
            throw new IllegalArgumentException("Could not find imports to remove: " + patterns);
        }
        return newLines;
    }

    /**
     * Remove imports if they match any of the patterns.
     */
    public static List<String> removeAnyImports(List<String> lines, String... importRegex) {
        final List<Pattern> patterns = Arrays.stream(importRegex).map(Pattern::compile).collect(Collectors.toList());
        return removeAnyImports(lines, patterns);
    }

    private static List<String> removeAnyImports(List<String> lines, List<Pattern> patterns) {
        final List<String> newLines = new ArrayList<>();


        NEXTLINE: for (String line : lines) {
            for (final Iterator<Pattern> it = patterns.iterator(); it.hasNext();) {
                final Pattern x = it.next();
                if (x.matcher(line).matches()) {
                    it.remove();
                    continue NEXTLINE;
                }
            }
            newLines.add(line);
        }

        return newLines;
    }

    private static String doLineReplacements(String x, String... replacements) {
        if (replacements.length % 2 != 0) {
            throw new IllegalStateException("Replacmement length is not even!");
        }
        for (int ii = 0; ii < replacements.length; ii += 2) {
            x = x.replaceAll(replacements[ii], replacements[ii + 1]);
        }
        return x;
    }

    @NotNull
    public static List<String> fixupChunkAttributes(List<String> lines) {
        return fixupChunkAttributes(lines, "Object");
    }

    @NotNull
    public static List<String> fixupChunkAttributes(List<String> lines, final String genericType) {
        lines = lines.stream().map(x -> x.replaceAll("ObjectChunk<([^>]*)>", "ObjectChunk<" + genericType + ", $1>"))
                .collect(Collectors.toList());
        return lines;
    }

    public static void fixupChunkAttributes(String objectPath) throws IOException {
        FileUtils.writeLines(new File(objectPath),
                fixupChunkAttributes(FileUtils.readLines(new File(objectPath), Charset.defaultCharset())));
    }

    public static List<String> indent(final List<String> lines, int spaces) {
        final char[] value = new char[spaces];
        Arrays.fill(value, ' ');
        final String spaceString = new String(value);
        return lines.stream().map(l -> spaceString + l).collect(Collectors.toList());
    }

    public static Map<String, List<String>> findNoLocateRegions(final String replicateDest) throws IOException {
        final File repDest = new File(replicateDest);
        if (!repDest.exists()) {
            return Collections.emptyMap();
        }

        final List<String> lines = FileUtils.readLines(repDest, Charset.defaultCharset());
        final Pattern startPattern = Pattern.compile("//\\s*region\\s+@NoReplicate\\s+([a-zA-Z0-9]+)");
        final Pattern endPattern = Pattern.compile("//\\s*endregion\\s+@NoReplicate\\s+([a-zA-Z0-9]+)");

        final Map<String, List<String>> noReplaceRegions = new HashMap<>();

        String curRegionName = null;
        List<String> linesForRegion = null;
        boolean inRegion = false;
        for (String line : lines) {
            final Matcher matcher = startPattern.matcher(line);
            if (matcher.find()) {
                if (inRegion) {
                    throw new IllegalStateException();
                }

                curRegionName = matcher.group(1);
                linesForRegion = new ArrayList<>();
                inRegion = true;
                continue;
            }

            final Matcher endMatcher = endPattern.matcher(line);
            if (endMatcher.find()) {
                if (!inRegion) {
                    throw new IllegalStateException();
                }

                if (!curRegionName.equals(endMatcher.group(1))) {
                    throw new IllegalStateException("End region clause does not match regin name");
                }

                inRegion = false;
                noReplaceRegions.put(curRegionName, linesForRegion);
                curRegionName = null;
                linesForRegion = null;
            }

            if (inRegion) {
                linesForRegion.add(line);
            }
        }

        if (inRegion) {
            throw new IllegalStateException("Region " + curRegionName + " never ended!");
        }

        return noReplaceRegions;
    }

    public static String fileHeaderString(String gradleTask, String sourceClassJavaPath) {
        Stream<String> fileHeaderStream = fileHeaderStream(gradleTask, sourceClassJavaPath);
        return fileHeaderStream.collect(Collectors.joining("\n"));
    }

    public static Stream<String> fileHeaderStream(String gradleTask, String sourceClassJavaPath) {
        return Stream.of("//",
                "// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending",
                "//",
                "// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY",
                "// ****** Edit " + sourceClassJavaPath + " and run \"./gradlew " + gradleTask
                        + "\" to regenerate",
                "//",
                "// @formatter:off",
                "");
    }

    @NotNull
    public static String className(@NotNull final String sourceClassJavaPath) {
        final String javaFileName = ReplicatePrimitiveCode.javaFileName(sourceClassJavaPath);
        return javaFileName.substring(0, javaFileName.length() - ".java".length());
    }
}
