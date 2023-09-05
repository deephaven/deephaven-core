/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.functions.BooleanFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import org.immutables.value.Value.Parameter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.functions.BooleanFunction.map;

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

    public static FieldPath empty() {
        return of(List.of());
    }

    public static FieldPath of(FieldDescriptor... descriptors) {
        return of(Arrays.asList(descriptors));
    }

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
    public static BooleanFunction<FieldPath> namePathStartsWithFieldPath(List<String> namePath) {
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
    public static BooleanFunction<FieldPath> simplePathStartsWithFieldPath(String simplePath) {
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
    public static BooleanFunction<FieldPath> anySimplePathStartsWithFieldPath(List<String> simplePaths) {
        return BooleanFunction
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

    @Parameter
    public abstract List<FieldDescriptor> path();

    @Lazy
    public FieldNumberPath numberPath() {
        return FieldNumberPath.of(path().stream().mapToInt(FieldDescriptor::getNumber).toArray());
    }

    @Lazy
    public List<String> namePath() {
        return path().stream().map(FieldDescriptor::getName).collect(Collectors.toList());
    }

    public final FieldPath prefixWith(FieldDescriptor prefix) {
        return FieldPath.of(Stream.concat(Stream.of(prefix), path().stream()).collect(Collectors.toList()));
    }

    public final FieldPath append(FieldDescriptor fieldDescriptor) {
        return FieldPath.of(Stream.concat(path().stream(), Stream.of(fieldDescriptor)).collect(Collectors.toList()));
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
