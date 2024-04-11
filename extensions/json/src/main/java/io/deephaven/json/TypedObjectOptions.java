//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A type-discriminated object.
 */
@Immutable
@BuildableStyle
public abstract class TypedObjectOptions extends ValueOptionsRestrictedUniverseBase {

    public static Builder builder() {
        return ImmutableTypedObjectOptions.builder();
    }

    /**
     * Creates a new builder with the {@link #typeFieldName()} set to {@code typeFieldName}, {@link #sharedFields()}
     * inferred from {@code objects} based on {@link ObjectFieldOptions} equality, and {@link #objects()} set to
     * {@code objects} with the shared fields removed.
     *
     * @param typeFieldName the type field name
     * @param objects the objects
     * @return the builder
     */
    public static Builder builder(String typeFieldName, Map<String, ObjectOptions> objects) {
        final Builder builder = builder().typeFieldName(typeFieldName);
        final Set<ObjectFieldOptions> sharedFields = new LinkedHashSet<>();
        final ObjectOptions first = objects.values().iterator().next();
        for (ObjectFieldOptions field : first.fields()) {
            boolean isShared = true;
            for (ObjectOptions obj : objects.values()) {
                if (!obj.fields().contains(field)) {
                    isShared = false;
                    break;
                }
            }
            if (isShared) {
                sharedFields.add(field);
            }
        }
        for (Entry<String, ObjectOptions> e : objects.entrySet()) {
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
    public static TypedObjectOptions standard(String typeFieldName, Map<String, ObjectOptions> objects) {
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
    public static TypedObjectOptions strict(String typeFieldName, Map<String, ObjectOptions> objects) {
        return builder(typeFieldName, objects)
                .allowUnknownTypes(false)
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.OBJECT)
                .build();
    }

    public abstract String typeFieldName();

    public abstract Set<ObjectFieldOptions> sharedFields();

    // canonical name
    public abstract Map<String, ObjectOptions> objects();

    /**
     * If unknown fields are allowed. By default is {@code true}.
     */
    @Default
    public boolean allowUnknownTypes() {
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

    public interface Builder extends ValueOptions.Builder<TypedObjectOptions, Builder> {

        Builder typeFieldName(String typeFieldName);

        Builder addSharedFields(ObjectFieldOptions element);

        Builder addSharedFields(ObjectFieldOptions... elements);

        Builder addAllSharedFields(Iterable<? extends ObjectFieldOptions> elements);

        Builder putObjects(String key, ObjectOptions value);

        Builder allowUnknownTypes(boolean allowUnknownTypes);
    }

    private static ObjectOptions without(ObjectOptions options, Set<ObjectFieldOptions> excludedFields) {
        final ObjectOptions.Builder builder = ObjectOptions.builder()
                .allowUnknownFields(options.allowUnknownFields())
                .allowMissing(options.allowMissing())
                .allowedTypes(options.allowedTypes());
        for (ObjectFieldOptions field : options.fields()) {
            if (!excludedFields.contains(field)) {
                builder.addFields(field);
            }
        }
        return builder.build();
    }
}
