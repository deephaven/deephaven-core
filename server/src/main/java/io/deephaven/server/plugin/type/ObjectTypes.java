package io.deephaven.server.plugin.type;

import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeCallback;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.plugin.type.ObjectTypeLookup;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Provides synchronized object type {@link ObjectTypeCallback registration} and {@link ObjectTypeLookup lookup}.
 *
 * <p>
 * Object type registration that is an instances of {@link ObjectTypeClassBase} receives special consideration, and
 * these objects have more efficient lookups.
 */
@Singleton
public final class ObjectTypes implements ObjectTypeLookup, ObjectTypeCallback {

    private static final Set<String> RESERVED_TYPE_NAMES = Set.of("Table", "TableMap", "TreeTable");

    private final Set<String> names;
    private final Map<Class<?>, ObjectType> classTypes;
    private final List<ObjectType> otherTypes;

    @Inject
    public ObjectTypes() {
        names = new HashSet<>();
        classTypes = new HashMap<>();
        otherTypes = new ArrayList<>();
    }

    @Override
    public synchronized Optional<ObjectType> findObjectType(Object object) {
        final ObjectType byClass = classTypes.get(object.getClass());
        if (byClass != null) {
            return Optional.of(byClass);
        }
        for (ObjectType type : otherTypes) {
            if (type.isType(object)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized void registerObjectType(ObjectType objectType) {
        if (isReservedName(objectType.name())) {
            throw new IllegalArgumentException("Unable to register type, name is reserved: " + objectType.name());
        }
        if (names.contains(objectType.name())) {
            throw new IllegalArgumentException(
                    "Unable to register type, type name already registered: " + objectType.name());
        }
        if (objectType instanceof ObjectTypeClassBase) {
            final Class<?> clazz = ((ObjectTypeClassBase<?>) objectType).clazz();
            if (classTypes.putIfAbsent(clazz, objectType) != null) {
                throw new IllegalArgumentException("Unable to register type, class already registered: " + clazz);
            }
        } else {
            otherTypes.add(objectType);
        }
        names.add(objectType.name());
    }

    private static boolean isReservedName(String name) {
        return RESERVED_TYPE_NAMES.contains(name);
    }
}
