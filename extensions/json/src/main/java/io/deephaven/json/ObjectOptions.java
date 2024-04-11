//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.ObjectFieldOptions.RepeatedBehavior;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

/**
 * Processes a JSON object as set of named fields. For example:
 *
 * <pre>
 * {
 *     "name": "Foo",
 *     "age": 42,
 *     "height": 6.5
 * }
 * </pre>
 */
@Immutable
@BuildableStyle
public abstract class ObjectOptions extends ValueOptionsRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableObjectOptions.builder();
    }

    /**
     * The lenient object options. The object fields are constructed with {@link ObjectFieldOptions#caseSensitive()} as
     * {@code false} and {@link ObjectFieldOptions#repeatedBehavior()} as
     * {@link ObjectFieldOptions.RepeatedBehavior#USE_FIRST}.
     *
     * @param fields the fields
     * @return the lenient object options
     */
    public static ObjectOptions lenient(Map<String, ValueOptions> fields) {
        final Builder builder = builder();
        for (Entry<String, ValueOptions> e : fields.entrySet()) {
            builder.addFields(ObjectFieldOptions.builder()
                    .name(e.getKey())
                    .options(e.getValue())
                    .caseSensitive(false)
                    .repeatedBehavior(RepeatedBehavior.USE_FIRST)
                    .build());
        }
        return builder.build();
    }

    /**
     * The standard object options.
     *
     * @param fields the fields
     * @return the standard object options
     */
    public static ObjectOptions standard(Map<String, ValueOptions> fields) {
        final Builder builder = builder();
        for (Entry<String, ValueOptions> e : fields.entrySet()) {
            builder.addFields(ObjectFieldOptions.of(e.getKey(), e.getValue()));
        }
        return builder.build();
    }

    /**
     * The strict object options.
     *
     * @param fields the fields
     * @return the strict object options
     */
    public static ObjectOptions strict(Map<String, ValueOptions> fields) {
        final Builder builder = builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.OBJECT);
        for (Entry<String, ValueOptions> e : fields.entrySet()) {
            builder.addFields(ObjectFieldOptions.of(e.getKey(), e.getValue()));
        }
        return builder.build();
    }

    /**
     * The fields.
     */
    public abstract Set<ObjectFieldOptions> fields();

    /**
     * If unknown fields are allowed. By default is {@code true}.
     */
    @Default
    public boolean allowUnknownFields() {
        return true;
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#OBJECT_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#OBJECT_OR_NULL}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    // not extending value options
    public interface Builder extends ValueOptions.Builder<ObjectOptions, Builder> {

        Builder allowUnknownFields(boolean allowUnknownFields);

        /**
         * A convenience method, equivalent to {@code addFields(ObjectFieldOptions.of(key, value))}.
         */
        default Builder putFields(String key, ValueOptions value) {
            return addFields(ObjectFieldOptions.of(key, value));
        }

        Builder addFields(ObjectFieldOptions element);

        Builder addFields(ObjectFieldOptions... elements);

        Builder addAllFields(Iterable<? extends ObjectFieldOptions> elements);
    }

    @Check
    final void checkNonOverlapping() {
        // We need to make sure there is no inter-field overlapping. We will be stricter if _any_ field is
        // case-insensitive.
        final Set<String> keys = fields().stream().allMatch(ObjectFieldOptions::caseSensitive)
                ? new HashSet<>()
                : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ObjectFieldOptions field : fields()) {
            if (!keys.add(field.name())) {
                throw new IllegalArgumentException(String.format("Found overlapping field name '%s'", field.name()));
            }
            for (String alias : field.aliases()) {
                if (!keys.add(alias)) {
                    throw new IllegalArgumentException(String.format("Found overlapping field alias '%s'", alias));
                }
            }
        }
    }
}
