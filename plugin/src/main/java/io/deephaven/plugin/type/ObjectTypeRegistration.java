//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.type;

/**
 * The {@link ObjectType} specific registration.
 */
public interface ObjectTypeRegistration {

    /**
     * Register {@code objectType}.
     *
     * @param objectType the object type
     */
    void register(ObjectType objectType);
}
