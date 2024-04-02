//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link CharVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class CharVectorDirectTest extends CharVectorTest {

    @Override
    protected CharVector makeTestVector(@NotNull final char... data) {
        return new CharVectorDirect(data);
    }
}
