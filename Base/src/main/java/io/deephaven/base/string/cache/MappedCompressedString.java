/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * This is a ridiculously simple, light-as-I-can-make-it, but decidedly single-purpose data structure. Specifically,
 * it's a CompressedString with an embedded (to avoid reference or Object instance overhead) open-addressed
 * SimpleReference<Object>-identity -> int hash map with load factor 1 (100%) and no public operations other than
 * "putIfAbsent".
 *
 * The reason for requiring that key objects be SimpleReferences is to allow for O(1) automatic slot reclamation across
 * many MappedCompressedString instances at the same time. A given source of mappings creates a single SimpleReference
 * to use as a key, and may invalidate mappings (creating a deleted slot) simply by clearing the SimpleReference. In
 * practice, a WeakSimpleReference to the source itself is used, in order to also allow garbage collection of the
 * mapping source object to invalidate all of its mappings.
 *
 * Unfortunately, I haven't figured out a way to make this allow concurrent gets.
 *
 * The intended use is in Deephaven import code, for storing SymbolManager -> SymbolId mappings on the CompressedString
 * that represents the Symbol itself, typically inside of a (bounded) StringCache of MappedCompressedString instances.
 *
 * Note that this uses io.deephaven.base.reference.SimpleReference instead of java.lang.ref.Reference so that unit tests
 * can avoid being required to use the concrete sub-classes of Reference, which all come with GC-related side-effects.
 */
public final class MappedCompressedString extends AbstractCompressedString<MappedCompressedString> {

    public static final int NULL_MAPPING_VALUE = -1;
    private static final int NULL_INDEX = -1;

    private transient SimpleReference<?> keys[] = new SimpleReference<?>[1];
    private transient int values[] = new int[1];

    public MappedCompressedString(String data) {
        super(data);
    }

    public MappedCompressedString(char[] data, int offset, int length) {
        super(data, offset, length);
    }

    public MappedCompressedString(char[] data) {
        super(data);
    }

    public MappedCompressedString(ByteBuffer data, int offset, int length) {
        super(data, offset, length);
    }

    public MappedCompressedString(ByteBuffer data) {
        super(data);
    }

    public MappedCompressedString(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public MappedCompressedString(byte[] data) {
        super(data);
    }

    @Override
    @NotNull
    public CompressedString toCompressedString() {
        return new CompressedString(getData());
    }

    @Override
    @NotNull
    public MappedCompressedString toMappedCompressedString() {
        return this;
    }

    @Override
    protected final MappedCompressedString convertValue(final String string) {
        return new MappedCompressedString(string);
    }

    @Override
    protected final MappedCompressedString convertValue(final byte[] data, final int offset, final int length) {
        return new MappedCompressedString(data, offset, length);
    }

    public final synchronized int capacity() {
        return keys.length;
    }

    /**
     * Add the specified <key, value> pair if no mapping already exists for key.
     * 
     * @param key A non-null Reference to an arbitrary object whose reachability determines mapping validity.
     * @param potentialValue The value to insert if none already exists. Must not equal NULL_MAPPING_VALUE.
     * @return The existing mapped value, if present, or NULL_MAPPING_VALUE if potentialValue was used.
     */
    public final synchronized int putIfAbsent(final SimpleReference<?> key, final int potentialValue) {
        return putIfAbsentInternal(Require.neqNull(key, "key"),
                Require.neq(potentialValue, "potentialValue", NULL_MAPPING_VALUE, "NULL_MAPPING_VALUE"), true);
    }

    private int putIfAbsentInternal(final SimpleReference<?> key, final int potentialValue, final boolean allowRehash) {
        final int firstIndex = firstIndexFor(key);
        int firstDeletedIndex = NULL_INDEX;

        SimpleReference<?> candidate = keys[firstIndex];
        if (candidate == key) {
            return values[firstIndex];
        }
        if (candidate == null) {
            keys[firstIndex] = key;
            values[firstIndex] = potentialValue;
            return NULL_MAPPING_VALUE;
        }
        if (candidate.get() == null) {
            firstDeletedIndex = firstIndex;
        }

        for (int ki = nextIndex(firstIndex); ki != firstIndex; ki = nextIndex(ki)) {
            candidate = keys[ki];
            if (candidate == key) {
                return values[ki];
            }
            if (candidate == null) {
                if (firstDeletedIndex != NULL_INDEX) {
                    keys[firstDeletedIndex] = key;
                    values[firstDeletedIndex] = potentialValue;
                    return NULL_MAPPING_VALUE;
                }
                keys[ki] = key;
                values[ki] = potentialValue;
                return NULL_MAPPING_VALUE;
            }
            if (firstDeletedIndex == NULL_INDEX && candidate.get() == null) {
                firstDeletedIndex = ki;
            }
        }

        if (firstDeletedIndex != NULL_INDEX) {
            keys[firstDeletedIndex] = key;
            values[firstDeletedIndex] = potentialValue;
            return NULL_MAPPING_VALUE;
        }

        if (allowRehash) {
            rehash();
            return putIfAbsentInternal(key, potentialValue, false);
        }
        throw new IllegalStateException(
                "BUG: No free space found for <" + key + ',' + potentialValue + ">, but allowRehash is false!");
    }

    private int firstIndexFor(final SimpleReference<?> key) {
        return System.identityHashCode(key) & (keys.length - 1);
    }

    private int nextIndex(final int index) {
        return (index + 1) & (keys.length - 1);
    }

    private void rehash() {
        final SimpleReference<?> oldKeys[] = keys;
        final int oldValues[] = values;

        keys = new SimpleReference<?>[oldKeys.length * 2];
        values = new int[keys.length];

        for (int oki = 0; oki < oldKeys.length; ++oki) {
            final SimpleReference<?> key = oldKeys[oki];
            if (key.get() == null) {
                continue;
            }
            if (putIfAbsentInternal(key, oldValues[oki], false) != NULL_MAPPING_VALUE) {
                throw new IllegalStateException("BUG: Mapping for <" + oldKeys[oki] + ',' + oldValues[oki]
                        + "> already present during rehash!");
            }
        }
    }
}
