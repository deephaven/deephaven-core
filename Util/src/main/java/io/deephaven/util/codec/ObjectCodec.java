package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * <p>
 * Codec interface for Object translation to/from byte arrays for serialization and deserialization.
 * <p>
 * Implementations must follow several rules to enable correct usage:
 * <ol>
 * <li>They must be stateless or designed for concurrent use (e.g. by using only ThreadLocal state),
 * as they will generally be cached and re-used.</li>
 * <li>They must not modify their inputs in any way, retain references to their inputs, or return
 * results that retain references to their inputs.</li>
 * <li>They should provide a public constructor that takes a single String argument, in order to
 * allow configuration-driven reflective instantiation.</li>
 * </ol>
 */
public interface ObjectCodec<TYPE> extends ObjectDecoder<TYPE> {

    /**
     * Encode the specified input as an array of bytes. Note that it is up to the implementation how
     * to encode null inputs. The use of a zero-length byte array (e.g.
     * {@link io.deephaven.datastructures.util.CollectionUtil#ZERO_LENGTH_BYTE_ARRAY}) is strongly
     * encouraged.
     *
     * @param input The input object, possibly null
     * @return The output byte array
     */
    @NotNull
    byte[] encode(@Nullable TYPE input);

    /**
     * Does this codec support encoding of null values?
     *
     * @return if null values are supported
     */
    boolean isNullable();

    /**
     * If applicable, the maximum encodable precision. If precision is not applicable (i.e. for
     * non-numeric types) this method should return zero.
     *
     * @return the numeric precision supported by this codec
     */
    int getPrecision();

    /**
     * If applicable, the maximum encodable scale. If scale is not applicable (i.e. for non-numeric
     * types) this method should return zero.
     *
     * @return the numeric scale (digits after the decimal point) supported by this codec
     */
    int getScale();
}
