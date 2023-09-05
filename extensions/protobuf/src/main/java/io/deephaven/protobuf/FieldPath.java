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
     * Creates a function that returns {@code true} if {@code namePath} starts with the {@link FieldPath#namePath()
     * field path's name path}.
     *
     * @param namePath the name path
     * @return the boolean function
     * @see FieldPath#otherStartsWithThis(List)
     */
    public static ToBooleanFunction<FieldPath> namePathStartsWithFieldPath(List<String> namePath) {
        return fieldPath -> fieldPath.otherStartsWithThis(namePath);
    }

    /**
     * Creates a function that returns {@code true} if the simple path starts with the {@link FieldPath#namePath() field
     * path's name path}. Equivalent to {@code namePathStartsWithFieldPath(toNamePath(simplePath))}.
     *
     * @param simplePath the simple path
     * @return the boolean function
     * @see #namePathStartsWithFieldPath(List)
     * @see #toNamePath(String)
     */
    public static ToBooleanFunction<FieldPath> simplePathStartsWithFieldPath(String simplePath) {
        return namePathStartsWithFieldPath(toNamePath(simplePath));
    }

    /**
     * Creates a function that returns {@code true} if any of the simple paths start with the
     * {@link FieldPath#namePath() field path's name path}.
     *
     * @param simplePaths the simple paths
     * @return the boolean function
     */
    @SuppressWarnings("unused")
    public static ToBooleanFunction<FieldPath> anySimplePathStartsWithFieldPath(List<String> simplePaths) {
        return ToBooleanFunction
                .or(simplePaths.stream().map(FieldPath::simplePathStartsWithFieldPath).collect(Collectors.toList()));
    }

    /**
     * Parse the simple path to a name path, treating `/` as the separating character.
     *
     * @param simplePath the simple path
     * @return the name path
     */
    public static List<String> toNamePath(String simplePath) {
        simplePath = !simplePath.isEmpty() && simplePath.charAt(0) == '/' ? simplePath.substring(1) : simplePath;
        return Arrays.asList(simplePath.split("/"));
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
