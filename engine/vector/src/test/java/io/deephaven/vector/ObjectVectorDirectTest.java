/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
