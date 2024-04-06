//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirectTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link ShortVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class ShortVectorDirectTest extends ShortVectorTest {

    @Override
    protected ShortVector makeTestVector(@NotNull final short... data) {
        return new ShortVectorDirect(data);
    }
}
