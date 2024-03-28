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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

/**
 * Processes a JSON object as map of processors.
 */
@Immutable
@BuildableStyle
public abstract class ObjectOptions extends ValueOptions {

    public static Builder builder() {
        return ImmutableObjectOptions.builder();
    }

    public static ObjectOptions lenient(Map<String, ValueOptions> fields) {
        final Builder builder = builder();
        for (Entry<String, ValueOptions> e : fields.entrySet()) {
            builder.addFields(ObjectFieldOptions.builder()
                    .name(e.getKey())
                    .options(e.getValue())
                    .caseInsensitiveMatch(true)
                    .build());
        }
        return builder.build();
    }

    public static ObjectOptions standard(Map<String, ValueOptions> fields) {
        final Builder builder = builder();
        for (Entry<String, ValueOptions> e : fields.entrySet()) {
            builder.addFields(ObjectFieldOptions.of(e.getKey(), e.getValue()));
        }
        return builder.build();
    }

    public static ObjectOptions strict(Map<String, ValueOptions> fields) {
        final Builder builder = builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.OBJECT);
        for (Entry<String, ValueOptions> e : fields.entrySet()) {
            builder.addFields(ObjectFieldOptions.builder()
                    .name(e.getKey())
                    .options(e.getValue())
                    .repeatedBehavior(RepeatedBehavior.ERROR)
                    .build());
        }
        return builder.build();
    }

    /**
     * The fields.
     */
    public abstract Set<ObjectFieldOptions> fields();

    /**
     * If unknown fields are allowed for {@code this} object.
     */
    @Default
    public boolean allowUnknownFields() {
        return true;
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
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

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.OBJECT_OR_NULL;
    }

    @Check
    final void checkNonOverlapping() {
        // We need to make sure there is no inter-field overlapping. We will be stricter if _any_ field is
        // case-insensitive.
        final Set<String> keys = fields().stream().anyMatch(ObjectFieldOptions::caseInsensitiveMatch)
                ? new TreeSet<>(String.CASE_INSENSITIVE_ORDER)
                : new HashSet<>();
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
