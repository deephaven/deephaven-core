package io.deephaven.server.plugin;

import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.plugin.type.ObjectTypeLookup;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class ObjectTypes implements ObjectTypeLookup {

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

    public synchronized void register(ObjectType type) {
        if (isReservedName(type.name())) {
            throw new IllegalArgumentException("Unable to register type, type name is reserved: " + type.name());
        }
        if (names.contains(type.name())) {
            throw new IllegalArgumentException(
                    "Unable to register type, type name already registered: " + type.name());
        }
        if (type instanceof ObjectTypeClassBase) {
            final Class<?> clazz = ((ObjectTypeClassBase<?>) type).clazz();
            if (classTypes.putIfAbsent(clazz, type) != null) {
                throw new IllegalArgumentException("Unable to register type, class already registered: " + clazz);
            }
        } else {
            otherTypes.add(type);
        }
        names.add(type.name());
    }

    private static boolean isReservedName(String name) {
        return RESERVED_TYPE_NAMES.contains(name);
    }
}
