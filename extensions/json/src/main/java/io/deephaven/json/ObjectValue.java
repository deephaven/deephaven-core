//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.ObjectField.RepeatedBehavior;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

/**
 * Processes a JSON object as set of named fields.
 *
 * <p>
 * For example, the following might be modelled as an object with a String "name" field, an int "age" field, and a
 * double "height" field:
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
public abstract class ObjectValue extends ValueRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableObjectValue.builder();
    }

    /**
     * The lenient object options. Allows missing, accepts {@link JsonValueTypes#objectOrNull()}, and allows unknown
     * fields. The object fields are constructed with {@link ObjectField#caseSensitive()} as {@code false} and
     * {@link ObjectField#repeatedBehavior()} as {@link ObjectField.RepeatedBehavior#USE_FIRST}.
     *
     * @param fields the fields
     * @return the lenient object options
     */
    public static ObjectValue lenient(Map<String, Value> fields) {
        final Builder builder = builder();
        for (Entry<String, Value> e : fields.entrySet()) {
            builder.addFields(ObjectField.builder()
                    .name(e.getKey())
                    .options(e.getValue())
                    .caseSensitive(false)
                    .repeatedBehavior(RepeatedBehavior.USE_FIRST)
                    .build());
        }
        return builder.build();
    }

    /**
     * The standard object options. Allows missing, accepts {@link JsonValueTypes#objectOrNull()}, and allows unknown
     * fields. The object fields are constructed with {@link ObjectField#of(String, Value)}.
     *
     * @param fields the fields
     * @return the standard object options
     */
    public static ObjectValue standard(Map<String, Value> fields) {
        final Builder builder = builder();
        for (Entry<String, Value> e : fields.entrySet()) {
            builder.putFields(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    /**
     * The strict object options. Disallows missing, accepts {@link JsonValueTypes#object()}, and disallows unknown
     * fields. The object fields are constructed with {@link ObjectField#of(String, Value)}.
     *
     * @param fields the fields
     * @return the strict object options
     */
    public static ObjectValue strict(Map<String, Value> fields) {
        final Builder builder = builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.object());
        for (Entry<String, Value> e : fields.entrySet()) {
            builder.putFields(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    /**
     * The fields.
     */
    public abstract Set<ObjectField> fields();

    /**
     * If unknown fields are allowed. By default is {@code true}.
     */
    @Default
    public boolean allowUnknownFields() {
        return true;
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#objectOrNull()}. By default is
     * {@link JsonValueTypes#objectOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.objectOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.objectOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    // not extending value options
    public interface Builder extends Value.Builder<ObjectValue, Builder> {

        Builder allowUnknownFields(boolean allowUnknownFields);

        /**
         * A convenience method, equivalent to {@code addFields(ObjectFieldOptions.of(key, value))}.
         */
        default Builder putFields(String key, Value value) {
            return addFields(ObjectField.of(key, value));
        }

        Builder addFields(ObjectField element);

        Builder addFields(ObjectField... elements);

        Builder addAllFields(Iterable<? extends ObjectField> elements);
    }

    @Check
    final void checkNonOverlapping() {
        // We need to make sure there is no inter-field overlapping. We will be stricter if _any_ field is
        // case-insensitive.
        final Set<String> keys = fields().stream().allMatch(ObjectField::caseSensitive)
                ? new HashSet<>()
                : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ObjectField field : fields()) {
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
