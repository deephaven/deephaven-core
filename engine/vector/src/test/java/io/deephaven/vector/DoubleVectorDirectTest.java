/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link DoubleVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class DoubleVectorDirectTest extends DoubleVectorTest {

    @Override
    protected DoubleVector makeTestVector(@NotNull final double... data) {
        return new DoubleVectorDirect(data);
    }
}
