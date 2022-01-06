package io.deephaven.plugin.type;

import java.util.Optional;

/**
 *
 */
public interface ObjectTypeLookup {

    /**
     * Find the {@link ObjectType} compatible with {@code object}. That is, {@link ObjectType#isType(Object)} will be
     * {@code true} for {@code object}.
     *
     * @param object the object
     * @return the object type, if found
     */
    Optional<ObjectType> findObjectType(Object object);

    enum NoOp implements ObjectTypeLookup {
        INSTANCE;

        @Override
        public Optional<ObjectType> findObjectType(Object object) {
            return Optional.empty();
        }
    }
}
