/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
