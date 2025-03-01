//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirectTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link ObjectVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class ObjectVectorDirectTest extends ObjectVectorTest {

    @Override
    protected ObjectVector<Object> makeTestVector(@NotNull final Object... data) {
        return new ObjectVectorDirect<>(data);
    }
}
