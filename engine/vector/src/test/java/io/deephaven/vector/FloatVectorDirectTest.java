/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link FloatVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class FloatVectorDirectTest extends FloatVectorTest {

    @Override
    protected FloatVector makeTestVector(@NotNull final float... data) {
        return new FloatVectorDirect(data);
    }
}
