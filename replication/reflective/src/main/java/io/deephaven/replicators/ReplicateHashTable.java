/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import gnu.trove.stack.TIntStack;
import gnu.trove.stack.array.TIntArrayStack;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.HashTableAnnotations.EmptyStateValue;
import io.deephaven.engine.table.impl.HashTableAnnotations.OverflowStateColumnSource;
import io.deephaven.engine.table.impl.HashTableAnnotations.StateColumnSource;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.deephaven.replication.ReplicatePrimitiveCode.fullClassName;

/**
 * This code replicator is designed to operate differently than the other replication in our system.
 *
 * It reads both the source and destination file, preserving custom code inside of the destination file.
 *
 * The source and destination files must provide annotations for their state column source, overflow column source, and
 * empty state value. These are used to translate names, and also to determine the appropriate types for substitution.
 *
 * The source file has three kinds of structured comments that control behavior.
 * <ul>
 * <li><b>regions</b>, denoted by <code>// region <i>name</i></code> and <code>// endregion <i>name</i></code> are
 * snippets of code that change between the source and destination. You should edit the code within a region in either
 * the source or destination file. Each region that exists in the source must exist in the destination (this is a sanity
 * check to prevent you from overwriting your work). Regions must have unique names.</li>
 *
 * <li><b>mixins</b>, denoted by <code>// mixin <i>name</i></code> and <code>// mixin <i>name</i></code> are snippets of
 * code that may not be useful in the destination class. Any mixins in the destination class will be overwritten! A
 * mixin can be spread across multiple structured blocks, for example imports and a function definition may both use the
 * same mixin name. Regions may exist inside a mixin. When mixins are excluded, the regions that exist within them are
 * ignored.</li>
 *
 * <li><b>substitutions</b>, denoted by <code>// @<i>thing</i> from <i>literal</i></code> are instructions to replace a
 * particular literal with the appropriate type denoted by thing on the next line. Multiple substitutions may be
 * separated using commas. The valid substitutions are:
 * <ul>
 * <li><b>StateChunkName</b>, e.g. "LongChunk"</li>
 * <li><b>StateChunkIdentityName</b>, e.g. "LongChunk" or "ObjectChunkIdentity"</li>
 * <li><b>StateChunkType</b>, e.g. "LongChunk&lt;Values&gt;"</li>
 * <li><b>WritableStateChunkName</b>, e.g. "WritableLongChunk"</li>
 * <li><b>WritableStateChunkType</b>, e.g. "WritableLongChunk&lt;Values&gt;"</li>
 * <li><b>StateColumnSourceType</b>, e.g. "LongArraySource"</li>
 * <li><b>StateColumnSourceConstructor</b>, e.g. "LongArraySource()"</li>
 * <li><b>NullStateValue</b>, e.g. "QueryConstants.NULL_LONG"</li>
 * <li><b>StateValueType</b>, e.g. "long"</li>
 * <li><b>StateChunkTypeEnum</b>, e.g. "Long"</li>
 * </ul>
 * </li>
 * </ul>
 */
public class ReplicateHashTable {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        final boolean allowMissingDestinations = false;

        // Incremental NJ -> Right-Incremental NJ
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/RightIncrementalChunkedNaturalJoinStateManager.java",
                allowMissingDestinations, Arrays.asList("rehash", "allowUpdateWriteThroughState", "dumpTable"));
        // Incremental NJ -> Static NJ
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/StaticChunkedNaturalJoinStateManager.java",
                allowMissingDestinations, Arrays.asList("rehash", "allowUpdateWriteThroughState", "dumpTable", "prev"));

        // Incremental NJ -> Static AJ
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/StaticChunkedAsOfJoinStateManager.java",
                allowMissingDestinations, Arrays.asList("dumpTable", "prev"));
        // Incremental NJ -> Right-Incremental AJ
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/RightIncrementalChunkedAsOfJoinStateManager.java",
                allowMissingDestinations, Collections.singletonList("dumpTable"));

        // Incremental NJ -> Symbol Table Combiner
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/SymbolTableCombiner.java",
                allowMissingDestinations, Arrays.asList("overflowLocationToHashLocation", "getStateValue", "prev"));

        // Incremental NJ -> Left-Only-Incremental Cross Join
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/LeftOnlyIncrementalChunkedCrossJoinStateManager.java",
                allowMissingDestinations, Collections.singletonList("dumpTable"));
        // Incremental NJ -> Right-Incremental Cross Join
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/RightIncrementalChunkedCrossJoinStateManager.java",
                allowMissingDestinations, Arrays.asList("dumpTable", "allowUpdateWriteThroughState"));
        // Incremental NJ -> Static Cross Join
        doReplicate(
                "engine/table/src/main/java/io/deephaven/engine/table/impl/IncrementalChunkedNaturalJoinStateManager.java",
                "engine/table/src/main/java/io/deephaven/engine/table/impl/StaticChunkedCrossJoinStateManager.java",
                allowMissingDestinations, Arrays.asList("dumpTable", "prev"));
    }

    private static class RegionedFile {
        final List<List<String>> noRegionSegments = new ArrayList<>();
        final List<String> regionNames = new ArrayList<>();
        final Map<String, List<String>> regionText = new LinkedHashMap<>();
    }

    private static class ColumnSourceInfo {
        String genericDataType;
        String stateColumnSourceRawType;
        ChunkType stateChunkType;
        String stateColumnSourceName;
        String overflowStateColumnSourceName;
        String emptyStateValue;

        String stateChunkType() {
            return writableStateChunkType().replace("Writable", "");
        }

        String stateChunkName() {
            return writableStateChunkName().replace("Writable", "");
        }

        String stateChunkIdentityName() {
            return writableStateChunkName().replace("Writable", "")
                    + (stateChunkType == ChunkType.Object ? "Identity" : "");
        }

        String writableStateChunkType() {
            return writableStateChunkName()
                    + (genericDataType == null ? "<Values>" : "<" + genericDataType + ",Values>");
        }

        String writableStateChunkName() {
            return stateChunkType.makeWritableChunk(0).getClass().getSimpleName();
        }

        String getStateColumnSourceType() {
            if (genericDataType == null) {
                return stateColumnSourceRawType;
            }
            return stateColumnSourceRawType + '<' + genericDataType + '>';
        }

        String getStateColumnSourceConstructor() {
            if (genericDataType == null) {
                return stateColumnSourceRawType + "()";
            }
            return stateColumnSourceRawType + "<>(" + genericDataType + ".class)";
        }

        String getNullStateValue() {
            if (stateChunkType == ChunkType.Object) {
                return "null";
            }
            return "QueryConstants.NULL_" + stateChunkType.name().toUpperCase();
        }

        String getStateValueType() {
            if (stateChunkType == ChunkType.Object) {
                return genericDataType;
            }
            return stateChunkType.name().toLowerCase();
        }

        String getStateChunkTypeEnum() {
            return stateChunkType.name();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void doReplicate(final String sourceClassJavaFile,
            final String destinationClassJavaFile,
            final boolean allowMissingDestinations,
            Collection<String> excludedMixins) throws IOException, ClassNotFoundException {
        final String sourceClassFullName = fullClassName(sourceClassJavaFile);
        final String destinationClassFullName = fullClassName(destinationClassJavaFile);
        final Class sourceClass = Class.forName(sourceClassFullName);
        final Class destinationClass = Class.forName(destinationClassFullName);

        final ColumnSourceInfo sourceColumnSourceInfo = findAnnotations(sourceClass);
        final ColumnSourceInfo destinationColumnSourceInfo = findAnnotations(destinationClass);


        final List<String> sourceLines = FileUtils.readLines(new File(sourceClassJavaFile), Charset.defaultCharset());
        final File destinationFile = new File(destinationClassJavaFile);
        final List<String> destLines = FileUtils.readLines(destinationFile, Charset.defaultCharset());

        final RegionedFile sourceRegioned = makeRegionedFile(sourceClassJavaFile, sourceLines, excludedMixins);
        final RegionedFile destRegioned = makeRegionedFile(destinationClassJavaFile, destLines, excludedMixins);

        final Set<String> missingInSource = new LinkedHashSet<>(destRegioned.regionNames);
        final Set<String> missingInDestination = new LinkedHashSet<>(sourceRegioned.regionNames);

        missingInSource.removeAll(sourceRegioned.regionNames);
        missingInDestination.removeAll(destRegioned.regionNames);

        if (!missingInSource.isEmpty()) {
            throw new IllegalStateException(destinationClassJavaFile + ": Region mismatch, not in source "
                    + missingInSource + ", not in destination" + missingInDestination);
        }
        if (!missingInDestination.isEmpty()) {
            if (allowMissingDestinations) {
                System.err.println("Allowing missing regions in destination: " + missingInDestination);
            } else {
                throw new IllegalStateException(destinationClassJavaFile + ": Region mismatch, not in source "
                        + missingInSource + ", not in destination" + missingInDestination);
            }
        }

        if (!allowMissingDestinations
                && sourceRegioned.noRegionSegments.size() != destRegioned.noRegionSegments.size()) {
            throw new IllegalStateException(
                    destinationClassJavaFile + ": Number of segments outside of regions does not match!");
        }

        final Function<String, String> replaceFunction = (sourceString) -> sourceString
                .replaceAll(sourceClass.getSimpleName(), destinationClass.getSimpleName())
                .replaceAll(sourceColumnSourceInfo.stateColumnSourceName,
                        destinationColumnSourceInfo.stateColumnSourceName)
                .replaceAll(sourceColumnSourceInfo.overflowStateColumnSourceName,
                        destinationColumnSourceInfo.overflowStateColumnSourceName)
                .replaceAll(sourceColumnSourceInfo.emptyStateValue, destinationColumnSourceInfo.emptyStateValue);

        final List<String> rewrittenLines = new ArrayList<>();
        for (int ii = 0; ii < sourceRegioned.noRegionSegments.size() - 1; ++ii) {
            final List<String> unregionedSegment = sourceRegioned.noRegionSegments.get(ii);
            final List<String> segmentLines =
                    rewriteSegment(destinationColumnSourceInfo, replaceFunction, unregionedSegment);

            rewrittenLines.addAll(segmentLines);

            final String regionName = sourceRegioned.regionNames.get(ii);
            final List<String> destinationRegion = destRegioned.regionText.get(regionName);
            if (destinationRegion == null) {
                if (allowMissingDestinations) {
                    rewrittenLines.addAll(sourceRegioned.regionText.get(regionName));
                } else {
                    throw new IllegalStateException();
                }
            } else {
                rewrittenLines.addAll(destinationRegion);
            }
        }
        final List<String> unregionedSegment =
                sourceRegioned.noRegionSegments.get(sourceRegioned.noRegionSegments.size() - 1);
        rewrittenLines.addAll(rewriteSegment(destinationColumnSourceInfo, replaceFunction, unregionedSegment));

        final String sourcePackage = sourceClass.getPackage().getName();
        final String destinationPackage = destinationClass.getPackage().getName();

        int packageLine;
        for (packageLine = 0; packageLine < 10; ++packageLine) {
            if (rewrittenLines.get(packageLine).startsWith("package")) {
                final String rewritePackage =
                        rewrittenLines.get(packageLine).replace(sourcePackage, destinationPackage);
                rewrittenLines.set(packageLine, rewritePackage);
                break;
            }
        }
        if (packageLine == 10) {
            throw new RuntimeException("Could not find package line to rewrite for " + destinationClass);
        }

        FileUtils.writeLines(destinationFile, rewrittenLines);
        System.out.println("Wrote: " + destinationClassJavaFile);
    }

    @NotNull
    private static List<String> rewriteSegment(ColumnSourceInfo destinationColumnSourceInfo,
            Function<String, String> replaceFunction, List<String> unregionedSegment) {
        final List<String> segmentLines = unregionedSegment.stream().map(replaceFunction).collect(Collectors.toList());

        final String replacementRegex = "@(\\S+)@\\s+from\\s+(\\S+)(\\s*,\\s*@\\S+@\\s+from\\s+\\S+)*\\s*";
        final Pattern controlPattern = Pattern.compile("(\\s*//\\s+)" + replacementRegex);
        final Pattern subsequentPattern = Pattern.compile(replacementRegex);
        for (int jj = 0; jj < segmentLines.size(); ++jj) {
            final String checkControl = segmentLines.get(jj);
            final Matcher matcher = controlPattern.matcher(checkControl);
            if (matcher.matches()) {
                if (jj == segmentLines.size() - 1) {
                    throw new IllegalStateException("Control on last line of unregioned segment!");
                }

                final StringBuilder controlReplacement = new StringBuilder(matcher.group(1));
                boolean firstControl = true;

                String replacementType = matcher.group(2);
                String fromReplacement = matcher.group(3);
                String subsequentReplacement = matcher.group(4);

                while (true) {
                    final String originalLine = segmentLines.get(jj + 1);
                    final String replacementValue;
                    switch (replacementType) {
                        case "StateChunkName":
                            replacementValue = destinationColumnSourceInfo.stateChunkName();
                            break;
                        case "StateChunkIdentityName":
                            replacementValue = destinationColumnSourceInfo.stateChunkIdentityName();
                            break;
                        case "StateChunkType":
                            replacementValue = destinationColumnSourceInfo.stateChunkType();
                            break;
                        case "WritableStateChunkName":
                            replacementValue = destinationColumnSourceInfo.writableStateChunkName();
                            break;
                        case "WritableStateChunkType":
                            replacementValue = destinationColumnSourceInfo.writableStateChunkType();
                            break;
                        case "StateColumnSourceType":
                            replacementValue = destinationColumnSourceInfo.getStateColumnSourceType();
                            break;
                        case "StateColumnSourceConstructor":
                            replacementValue = destinationColumnSourceInfo.getStateColumnSourceConstructor();
                            break;
                        case "NullStateValue":
                            replacementValue = destinationColumnSourceInfo.getNullStateValue();
                            break;
                        case "StateValueType":
                            replacementValue = destinationColumnSourceInfo.getStateValueType();
                            break;
                        case "StateChunkTypeEnum":
                            replacementValue = destinationColumnSourceInfo.getStateChunkTypeEnum();
                            break;
                        default:
                            throw new IllegalStateException("Unknown replacement: " + replacementType);
                    }
                    controlReplacement.append(firstControl ? "" : ", ").append('@').append(replacementType)
                            .append("@ from ").append(Pattern.quote(replacementValue));
                    firstControl = false;

                    final String replacementLine = originalLine.replaceAll(fromReplacement, replacementValue);
                    segmentLines.set(jj + 1, replacementLine);
                    if (subsequentReplacement == null) {
                        break;
                    }
                    final Matcher subsequentMatcher =
                            subsequentPattern.matcher(subsequentReplacement.replaceFirst("\\s*,\\s*", ""));
                    if (!subsequentMatcher.matches()) {
                        throw new IllegalStateException("Invalid subsequent replacement: " + subsequentReplacement);
                    }
                    replacementType = subsequentMatcher.group(1);
                    fromReplacement = subsequentMatcher.group(2);
                    subsequentReplacement = subsequentMatcher.group(3);
                }
                segmentLines.set(jj, controlReplacement.toString());
                jj++;
            }
        }
        return segmentLines;
    }

    private static ColumnSourceInfo findAnnotations(Class<?> clazz) {
        final ColumnSourceInfo result = new ColumnSourceInfo();

        final Field[] fields = clazz.getDeclaredFields();

        final Field stateColumnSourceField = findAnnotatedField(clazz, fields, StateColumnSource.class);
        final Class<?> type = stateColumnSourceField.getType();
        if (ColumnSource.class.isAssignableFrom(type)) {
            final Type genericType = stateColumnSourceField.getGenericType();
            if (genericType instanceof ParameterizedType) {
                final ParameterizedType parameterizedType = (ParameterizedType) genericType;
                final Class dataType = (Class) ((ParameterizedType) genericType).getActualTypeArguments()[0];
                result.genericDataType = dataType.getSimpleName();
                // noinspection unchecked
                final Class<ColumnSource> asColumnSource = (Class<ColumnSource>) parameterizedType.getRawType();
                try {
                    result.stateColumnSourceRawType = asColumnSource.getSimpleName();
                    final ColumnSource cs = asColumnSource.getConstructor(Class.class).newInstance(dataType);
                    result.stateChunkType = cs.getChunkType();
                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException
                        | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            } else { // type instanceof Class
                // noinspection unchecked
                final Class<ColumnSource> asColumnSource = (Class<ColumnSource>) type;
                try {
                    result.stateColumnSourceRawType = asColumnSource.getSimpleName();
                    final ColumnSource cs = asColumnSource.newInstance();
                    result.stateChunkType = cs.getChunkType();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            throw new IllegalStateException("Not a column source: field=" + stateColumnSourceField + ", type=" + type);
        }

        result.stateColumnSourceName = stateColumnSourceField.getName();
        result.overflowStateColumnSourceName =
                findAnnotatedField(clazz, fields, OverflowStateColumnSource.class).getName();
        result.emptyStateValue = findAnnotatedField(clazz, fields, EmptyStateValue.class).getName();

        return result;
    }

    @NotNull
    private static Field findAnnotatedField(Class<?> clazz, Field[] fields,
            Class<? extends Annotation> annotationClass) {
        final List<Field> matchingFields = Arrays.stream(fields).filter(f -> f.getAnnotation(annotationClass) != null)
                .collect(Collectors.toList());
        if (matchingFields.size() > 1) {
            throw new RuntimeException("Multiple fields annotated with " + annotationClass.getSimpleName()
                    + " annotation in " + clazz.getCanonicalName());
        }
        if (matchingFields.size() < 1) {
            throw new RuntimeException("Could not find annotation with " + annotationClass.getSimpleName()
                    + " annotation in " + clazz.getCanonicalName());
        }
        return matchingFields.get(0);
    }

    private static RegionedFile makeRegionedFile(final String name, List<String> lines,
            Collection<String> excludedMixins) {
        final Pattern startMixinPattern = Pattern.compile("\\s*//\\s+mixin\\s+(.*)?\\s*");
        final Pattern endMixinPattern = Pattern.compile("\\s*//\\s+endmixin\\s+(.*)?\\s*");
        final Pattern altMixinPattern = Pattern.compile("(\\s*)//\\s+altmixin\\s+(.*?):\\s(.*?)(\\\\)?");

        final Pattern startRegionPattern = Pattern.compile("\\s*//\\s+region\\s+(.*)?\\s*");
        final Pattern endRegionPattern = Pattern.compile("\\s*//\\s+endregion\\s+(.*)?\\s*");

        int regionOpenLine = 0;

        String currentRegion = null;
        final Stack<String> mixinStack = new Stack<>();
        final TIntStack mixinOpenLine = new TIntArrayStack();
        String currentMixin = null;

        final RegionedFile result = new RegionedFile();

        List<String> accumulated = new ArrayList<>();
        boolean inExclude = false;

        for (int lineNumber = 1; lineNumber <= lines.size(); ++lineNumber) {
            String line = lines.get(lineNumber - 1);

            final Matcher altMixinMatcher = altMixinPattern.matcher(line);
            if (altMixinMatcher.matches()) {
                if (excludedMixins.contains(altMixinMatcher.group(2))) {
                    line = altMixinMatcher.group(1) + altMixinMatcher.group(3);
                    if (altMixinMatcher.group(4) != null) {
                        line += lines.get(lineNumber++).replaceFirst("\\s+", "");
                    }
                }
            }

            final Matcher mixinEndMatcher = endMixinPattern.matcher(line);
            if (mixinEndMatcher.matches()) {
                if (currentRegion != null) {
                    throw new IllegalStateException(
                            name + ":" + lineNumber + ": Can not end mixin while a region is open,  " + currentRegion
                                    + " opened at line " + regionOpenLine);
                }
                if (currentMixin == null) {
                    throw new IllegalStateException(
                            name + ":" + lineNumber + ": Can not end mixin without an open mixin.");
                }
                if (!currentMixin.equals(mixinEndMatcher.group(1))) {
                    throw new IllegalStateException(name + ":" + lineNumber + ": ended mixin "
                            + mixinEndMatcher.group(1) + ", but current mixin is " + currentMixin);
                }
                mixinStack.pop();
                currentMixin = mixinStack.isEmpty() ? null : mixinStack.peek();
                mixinOpenLine.pop();
                final boolean oldInExclude = inExclude;
                inExclude = excludedMixins.contains(currentMixin);
                if (oldInExclude) {
                    continue;
                }
            }
            final Matcher mixinStartMatcher = startMixinPattern.matcher(line);
            if (mixinStartMatcher.matches()) {
                if (currentRegion != null) {
                    throw new IllegalStateException(
                            name + ":" + lineNumber + ": Can not start mixin while a region is open,  " + currentRegion
                                    + " opened at line " + regionOpenLine);
                }
                currentMixin = mixinStartMatcher.group(1);
                mixinStack.push(currentMixin);
                mixinOpenLine.push(lineNumber);
                inExclude = excludedMixins.contains(currentMixin);
            }
            if (inExclude) {
                continue;
            }


            final Matcher regionStartMatcher = startRegionPattern.matcher(line);
            if (regionStartMatcher.matches()) {
                result.noRegionSegments.add(accumulated);
                accumulated = new ArrayList<>();
                if (currentRegion != null) {
                    throw new IllegalStateException(name + ":" + lineNumber + ": Already in region " + currentRegion
                            + " opened at line" + regionOpenLine);
                }
                currentRegion = regionStartMatcher.group(1);
                regionOpenLine = lineNumber;
                if (result.regionText.containsKey(currentRegion)) {
                    throw new IllegalStateException(
                            name + ":" + lineNumber + ": Multiply defined region " + currentRegion + ".");
                }
            }
            final Matcher regionEndMatcher = endRegionPattern.matcher(line);
            if (regionEndMatcher.matches()) {
                if (currentRegion == null) {
                    throw new IllegalStateException(
                            name + ":" + lineNumber + ": not in region, but encountered " + line);
                }
                if (!currentRegion.equals(regionEndMatcher.group(1))) {
                    throw new IllegalStateException(name + ":" + lineNumber + ": ended region "
                            + regionEndMatcher.group(1) + ", but current region is " + currentRegion);
                }
                result.regionNames.add(currentRegion);
                result.regionText.put(currentRegion, accumulated);
                accumulated = new ArrayList<>();
                currentRegion = null;
            }
            accumulated.add(line);
        }

        if (currentRegion != null) {
            throw new IllegalStateException("Region " + currentRegion + " never ended!");
        }

        if (currentMixin != null) {
            throw new IllegalStateException(
                    "Mixin " + currentMixin + " never ended, started on line " + mixinOpenLine.peek());
        }

        result.noRegionSegments.add(accumulated);

        Require.eq(result.noRegionSegments.size() - 1, "result.noRegionSegments.size() - 1", result.regionText.size(),
                "result.regionText.size()");

        return result;
    }
}
