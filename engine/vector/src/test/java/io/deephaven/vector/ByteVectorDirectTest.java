/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorDirectTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * {@link ByteVectorTest} implementation for "direct" vectors and derivations thereof.
 */
public class ByteVectorDirectTest extends ByteVectorTest {

    @Override
    protected ByteVector makeTestVector(@NotNull final byte... data) {
        return new ByteVectorDirect(data);
    }
}
