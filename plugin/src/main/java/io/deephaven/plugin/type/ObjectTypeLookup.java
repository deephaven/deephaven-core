package io.deephaven.plugin.type;

import java.util.Optional;

public interface ObjectTypeLookup {

    Optional<ObjectType> findObjectType(Object object);

    enum NoOp implements ObjectTypeLookup {
        INSTANCE;

        @Override
        public Optional<ObjectType> findObjectType(Object object) {
            return Optional.empty();
        }
    }
}
