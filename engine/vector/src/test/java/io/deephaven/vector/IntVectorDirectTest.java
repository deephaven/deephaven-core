/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link IntVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class IntVectorDirectTest extends IntVectorTest {

    @Override
    protected IntVector makeTestVector(@NotNull final int... data) {
        return new IntVectorDirect(data);
    }
}
