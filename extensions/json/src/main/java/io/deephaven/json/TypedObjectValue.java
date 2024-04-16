//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A type-discriminated object is a JSON object whose fields depend on a specific type field.
 *
 * <p>
 * For example, the following might be modelled as a type-discriminated object with "type" as the type field, "symbol"
 * as a shared field, with a "trade" object containing a "bid" and an "ask" field, and with a "quote" object containing
 * a "price" and a "size" field:
 *
 * <pre>
 * {
 *   "type": "trade",
 *   "symbol": "FOO",
 *   "price": 70.03,
 *   "size": 42
 * }
 * </pre>
 * 
 * <pre>
 * {
 *   "type": "quote",
 *   "symbol": "BAR",
 *   "bid": 10.01,
 *   "ask": 10.05
 * }
 * </pre>
 */
@Immutable
@BuildableStyle
public abstract class TypedObjectValue extends ValueRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableTypedObjectValue.builder();
    }

    /**
     * Creates a new builder with the {@link #typeFieldName()} set to {@code typeFieldName}, {@link #sharedFields()}
     * inferred from {@code objects} based on {@link ObjectField} equality, and {@link #objects()} set to
     * {@code objects} with the shared fields removed.
     *
     * @param typeFieldName the type field name
     * @param objects the objects
     * @return the builder
     */
    public static Builder builder(String typeFieldName, Map<String, ObjectValue> objects) {
        final Builder builder = builder().typeFieldName(typeFieldName);
        final Set<ObjectField> sharedFields = new LinkedHashSet<>();
        final ObjectValue first = objects.values().iterator().next();
        for (ObjectField field : first.fields()) {
            boolean isShared = true;
            for (ObjectValue obj : objects.values()) {
                if (!obj.fields().contains(field)) {
                    isShared = false;
                    break;
                }
            }
            if (isShared) {
                sharedFields.add(field);
            }
        }
        for (Entry<String, ObjectValue> e : objects.entrySet()) {
            builder.putObjects(e.getKey(), without(e.getValue(), sharedFields));
        }
        return builder.addAllSharedFields(sharedFields);
    }

    /**
     * Creates a typed object by inferring the shared fields. Equivalent to
     * {@code builder(typeFieldName, objects).build()}.
     *
     * @param typeFieldName the type field name
     * @param objects the objects
     * @return the typed object
     */
    public static TypedObjectValue standard(String typeFieldName, Map<String, ObjectValue> objects) {
        return builder(typeFieldName, objects).build();
    }

    /**
     * Creates a typed object by inferring the shared fields. Equivalent to
     * {@code builder(typeFieldName, objects).allowUnknownTypes(false).allowMissing(false).desiredTypes(JsonValueTypes.OBJECT).build()}.
     *
     * @param typeFieldName the type field name
     * @param objects the objects
     * @return the typed object
     */
    public static TypedObjectValue strict(String typeFieldName, Map<String, ObjectValue> objects) {
        return builder(typeFieldName, objects)
                .allowUnknownTypes(false)
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.object())
                .build();
    }

    public abstract String typeFieldName();

    public abstract Set<ObjectField> sharedFields();

    // canonical name
    public abstract Map<String, ObjectValue> objects();

    /**
     * If unknown fields are allowed. By default is {@code true}.
     */
    @Default
    public boolean allowUnknownTypes() {
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

    public interface Builder extends Value.Builder<TypedObjectValue, Builder> {

        Builder typeFieldName(String typeFieldName);

        Builder addSharedFields(ObjectField element);

        Builder addSharedFields(ObjectField... elements);

        Builder addAllSharedFields(Iterable<? extends ObjectField> elements);

        Builder putObjects(String key, ObjectValue value);

        Builder allowUnknownTypes(boolean allowUnknownTypes);
    }

    private static ObjectValue without(ObjectValue options, Set<ObjectField> excludedFields) {
        final ObjectValue.Builder builder = ObjectValue.builder()
                .allowUnknownFields(options.allowUnknownFields())
                .allowMissing(options.allowMissing())
                .allowedTypes(options.allowedTypes());
        for (ObjectField field : options.fields()) {
            if (!excludedFields.contains(field)) {
                builder.addFields(field);
            }
        }
        return builder.build();
    }
}
