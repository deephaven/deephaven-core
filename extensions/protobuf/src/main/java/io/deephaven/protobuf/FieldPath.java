/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.functions.ToBooleanFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import org.immutables.value.Value.Parameter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link #path()} to a {@link com.google.protobuf.Descriptors.Descriptor Descriptor's} field.
 *
 * <p>
 * {@link FieldDescriptor} objects are not equal across {@link com.google.protobuf.DynamicMessage dynamic messages}.
 * Helpers {@link #numberPath()} and {@link #namePath()} may be used in lieu of direct equivalence depending on context.
 */
@Immutable
@SimpleStyle
public abstract class FieldPath {

    private static final FieldPath EMPTY = of(List.of());

    /**
     * Creates an empty field path. Equivalent to {@code of(List.of())}.
     *
     * @return the empty field path
     */
    public static FieldPath empty() {
        return EMPTY;
    }

    /**
     * Creates a field path with {@code descriptors}.
     *
     * @param descriptors the descriptors
     * @return the field path
     */
    public static FieldPath of(FieldDescriptor... descriptors) {
        return of(Arrays.asList(descriptors));
    }

    /**
     * Creates a field path with {@code descriptors}.
     *
     * @param descriptors the descriptors
     * @return the field path
     */
    public static FieldPath of(List<FieldDescriptor> descriptors) {
        return ImmutableFieldPath.of(descriptors);
    }

    /**
     * Creates a boolean function according to {@code simplePath}, where {@code simplePath} represents a
     * {@link FieldPath#namePath() name path} that is '/' separated. The final name path part may be a '*' to
     * additionally match everything that starts with {@code simplePath}.
     *
     * <p>
     * For example, {@code simplePath="/foo/bar"} will provide a function that matches the field path name paths
     * {@code []}, {@code ["foo"]}, and {@code ["foo", "bar"]}. {@code simplePath="/foo/bar/*"} will provide a function
     * that matches the previous example, as well as any field path name paths that start with {@code ["foo", "bar"]}:
     * {@code ["foo", "bar", "baz"]}, {@code ["foo", "bar", "baz", "zap"}, {@code ["foo", "bar", "zip"]}, etc.
     *
     * @param simplePath the simple path
     * @return the field path function
     */
    public static ToBooleanFunction<FieldPath> matches(String simplePath) {
        simplePath = !simplePath.isEmpty() && simplePath.charAt(0) == '/' ? simplePath.substring(1) : simplePath;
        final List<String> np = Arrays.asList(simplePath.split("/"));
        final boolean star = !np.isEmpty() && "*".equals(np.get(np.size() - 1));
        final List<String> namePath = star ? np.subList(0, np.size() - 1) : np;
        // This matches everything leading up to the name path.
        // For example, simplePath=/foo/bar, namePath=[foo, bar], this will match field paths:
        // [], [foo], and [foo, bar].
        final ToBooleanFunction<FieldPath> leadingUpToMatch = fieldPath -> fieldPath.otherStartsWithThis(namePath);
        if (!star) {
            return leadingUpToMatch;
        }
        // This matches everything at or after name path in the case of a star.
        // For example, simplePath=/foo/bar/*, namePath=[foo, bar], this will match field paths:
        // [foo, bar], [foo, bar, baz], [foo, bar, baz, zip], etc.
        final ToBooleanFunction<FieldPath> starMatch = fieldPath -> fieldPath.startsWith(namePath);
        return ToBooleanFunction.or(List.of(leadingUpToMatch, starMatch));
    }

    /**
     * Creates a boolean function that is {@code true} when any component of {@code simplePaths} would
     * {@link #matches(String)}.
     *
     * <p>
     * Equivalent to
     * {@code ToBooleanFunction.or(simplePaths.stream().map(FieldPath::matches).collect(Collectors.toList()))}.
     *
     * @param simplePaths the simple paths
     * @return the field path function
     */
    public static ToBooleanFunction<FieldPath> anyMatches(List<String> simplePaths) {
        return ToBooleanFunction.or(simplePaths.stream().map(FieldPath::matches).collect(Collectors.toList()));
    }

    /**
     * The ordered field descriptors which make up the field path.
     *
     * @return the path
     */
    @Parameter
    public abstract List<FieldDescriptor> path();

    /**
     * The number path for this field path. Equivalent to
     * {@code FieldNumberPath.of(path().stream().mapToInt(FieldDescriptor::getNumber).toArray())}.
     *
     * @return the number path
     */
    @Lazy
    public FieldNumberPath numberPath() {
        return FieldNumberPath.of(path().stream().mapToInt(FieldDescriptor::getNumber).toArray());
    }

    /**
     * The name path for this field path. Equivalent to
     * {@code path().stream().map(FieldDescriptor::getName).collect(Collectors.toList())}.
     *
     * @return the name path
     **/
    @Lazy
    public List<String> namePath() {
        return path().stream().map(FieldDescriptor::getName).collect(Collectors.toList());
    }

    public final boolean startsWith(List<String> prefix) {
        return startsWith(namePath(), prefix);
    }

    public final boolean otherStartsWithThis(List<String> other) {
        return startsWith(other, namePath());
    }

    private static boolean startsWith(List<String> x, List<String> prefix) {
        return x.subList(0, Math.min(prefix.size(), x.size())).equals(prefix);
    }
}
