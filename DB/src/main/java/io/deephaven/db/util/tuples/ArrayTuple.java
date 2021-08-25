package io.deephaven.db.util.tuples;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.lang.DBLanguageFunctionUtil;
import io.deephaven.db.util.serialization.SerializationUtils;
import io.deephaven.db.util.serialization.StreamingExternalizable;
import gnu.trove.map.TIntObjectMap;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.Arrays;
import java.util.function.UnaryOperator;

/**
 * <p>
 * N-Tuple key class backed by an array of elements.
 */
public class ArrayTuple
        implements Comparable<ArrayTuple>, Externalizable, StreamingExternalizable, CanonicalizableTuple<ArrayTuple> {

    private static final long serialVersionUID = 1L;

    private Object elements[];

    private transient int cachedHashCode;

    /**
     * Construct a tuple backed by the supplied array of elements. The elements array should not be changed after this
     * call.
     *
     * @param elements The array to wrap
     */
    public ArrayTuple(final Object... elements) {
        initialize(elements == null ? CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY : elements);
    }

    /**
     * This is required for {@link Externalizable} support, but should not be used otherwise.
     */
    public ArrayTuple() {}

    private void initialize(@NotNull final Object elements[]) {
        this.elements = elements;
        cachedHashCode = Arrays.hashCode(elements);
    }

    @SuppressWarnings("unused")
    public final <T> T getElement(final int elementIndex) {
        // noinspection unchecked
        return (T) elements[elementIndex];
    }

    /**
     * Return a new array of the elements of this tuple, safe for any use.
     *
     * @return A new array of the elements of this tuple
     */
    public final Object[] getElements() {
        final Object exportedElements[] = new Object[elements.length];
        System.arraycopy(elements, 0, exportedElements, 0, elements.length);
        return exportedElements;
    }

    @Override
    public final int hashCode() {
        return cachedHashCode;
    }

    @Override
    public final boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ArrayTuple typedOther = (ArrayTuple) other;
        return Arrays.equals(elements, typedOther.elements);
    }

    @Override
    public final int compareTo(@NotNull final ArrayTuple other) {
        if (this == other) {
            return 0;
        }
        final int thisLength = elements.length;
        if (thisLength != other.elements.length) {
            throw new IllegalArgumentException("Mismatched lengths in " + ArrayTuple.class.getSimpleName() +
                    " comparison (this.elements=" + Arrays.toString(elements) +
                    ", other.elements=" + Arrays.toString(other.elements) + ')');
        }
        for (int ei = 0; ei < thisLength; ++ei) {
            final int comparison =
                    DBLanguageFunctionUtil.compareTo((Comparable) elements[ei], (Comparable) other.elements[ei]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        out.writeInt(elements.length);
        for (final Object element : elements) {
            out.writeObject(element);
        }
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {
        final int inLength = in.readInt();
        final Object inElements[] = new Object[inLength];
        for (int ei = 0; ei < inLength; ++ei) {
            inElements[ei] = in.readObject();
        }
        initialize(inElements);
    }

    @Override
    public void writeExternalStreaming(@NotNull final ObjectOutput out,
            @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException {
        final int length = elements.length;
        out.writeInt(length);
        for (int ei = 0; ei < length; ++ei) {
            StreamingExternalizable.writeObjectElement(out, cachedWriters, ei, elements[ei]);
        }
    }

    @Override
    public void readExternalStreaming(@NotNull final ObjectInput in,
            @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {
        final int inLength = in.readInt();
        final Object inElements[] = new Object[inLength];
        for (int ei = 0; ei < inLength; ++ei) {
            inElements[ei] = StreamingExternalizable.readObjectElement(in, cachedReaders, ei);
        }
        initialize(inElements);
    }

    @Override
    public String toString() {
        return "ArrayTuple{" + Arrays.toString(elements) + '}';
    }

    @Override
    public ArrayTuple canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {
        final int thisLength = elements.length;
        Object canonicalElements[] = null;
        for (int ei = 0; ei < thisLength; ++ei) {
            final Object element = elements[ei];
            final Object canonicalElement = canonicalizer.apply(element);
            if (canonicalElements == null && element != canonicalElement) {
                canonicalElements = new Object[thisLength];
                System.arraycopy(elements, 0, canonicalElements, 0, ei);
            }
            if (canonicalElements != null) {
                canonicalElements[ei] = canonicalElement;
            }
        }
        return canonicalElements == null ? this : new ArrayTuple(canonicalElements);
    }
}
