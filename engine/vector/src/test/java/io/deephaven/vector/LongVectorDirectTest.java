/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link LongVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class LongVectorDirectTest extends LongVectorTest {

    @Override
    protected LongVector makeTestVector(@NotNull final long... data) {
        return new LongVectorDirect(data);
    }
}
