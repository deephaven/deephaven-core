//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

@Immutable
@BuildableStyle
public abstract class ObjectField {

    public static Builder builder() {
        return ImmutableObjectField.builder();
    }

    /**
     * Creates a field with a single {@code name}. Equivalent to {@code builder().name(name).options(options).build()}.
     *
     * @param name the name
     * @param options the options
     * @return the field options
     */
    public static ObjectField of(String name, Value options) {
        return builder().name(name).options(options).build();
    }

    /**
     * The canonical field name.
     */
    public abstract String name();

    /**
     * The value options.
     */
    public abstract Value options();

    /**
     * The field name aliases.
     */
    public abstract Set<String> aliases();

    /**
     * If the field name and aliases should be compared using case-sensitive equality. By default is {@code true}.
     */
    @Default
    public boolean caseSensitive() {
        return true;
    }

    /**
     * The behavior when a repeated field is encountered. By default is {@link RepeatedBehavior#ERROR}.
     */
    @Default
    public RepeatedBehavior repeatedBehavior() {
        return RepeatedBehavior.ERROR;
    }

    /**
     * The array group for {@code this} field. This is useful in scenarios where {@code this} field's array is
     * guaranteed to have the same cardinality as one or more other array fields. For example, in the following snippet,
     * we might model "prices" and "quantities" as having the same array group:
     *
     * <pre>
     * {
     *   "prices": [1.1, 2.2, 3.3],
     *   "quantities": [9, 5, 42]
     * }
     * </pre>
     */
    public abstract Optional<Object> arrayGroup();

    /**
     * The behavior when a repeated field is encountered in a JSON object. For example, as in:
     *
     * <pre>
     * {
     *   "foo": 1,
     *   "foo": 2
     * }
     * </pre>
     */
    public enum RepeatedBehavior {
        /**
         * Throws an error if a repeated field is encountered
         */
        ERROR,

        /**
         * Uses the first field of a given name, ignores the rest
         */
        USE_FIRST,

        // /**
        // * Uses the last field of a given name, ignores the rest. Not currently supported.
        // */
        // USE_LAST
    }

    public interface Builder {
        Builder name(String name);

        Builder options(Value options);

        Builder addAliases(String element);

        Builder addAliases(String... elements);

        Builder addAllAliases(Iterable<String> elements);

        Builder repeatedBehavior(RepeatedBehavior repeatedBehavior);

        Builder caseSensitive(boolean caseSensitive);

        Builder arrayGroup(Object arrayGroup);

        ObjectField build();
    }

    @Check
    final void checkNonOverlapping() {
        if (caseSensitive()) {
            if (aliases().contains(name())) {
                throw new IllegalArgumentException(
                        String.format("name and aliases must be non-overlapping, found '%s' overlaps", name()));
            }
        } else {
            final Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            set.add(name());
            for (String alias : aliases()) {
                if (!set.add(alias)) {
                    throw new IllegalArgumentException(
                            String.format("name and aliases must be non-overlapping, found '%s' overlaps", alias));
                }
            }
        }
    }

    @Check
    final void checkArrayGroup() {
        if (arrayGroup().isEmpty()) {
            return;
        }
        if (!(options() instanceof ArrayValue)) {
            throw new IllegalArgumentException("arrayGroup is only valid with ArrayOptions");
        }
    }
}
