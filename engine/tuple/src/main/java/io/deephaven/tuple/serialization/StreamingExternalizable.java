package io.deephaven.tuple.serialization;

import gnu.trove.map.TIntObjectMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Interface for "tuple" classes of variably-typed elements that support an alternative to
 * {@link java.io.Externalizable} for writing in streaming fashion.
 */
public interface StreamingExternalizable {

    /**
     * <p>
     * Alternative to {@link java.io.Externalizable#writeExternal(ObjectOutput)} for use when a series of tuples of the
     * same type with the same element types are being written in iterative fashion.
     * <p>
     * Primitive elements should be written with their primitive write methods (e.g.
     * {@link ObjectOutput#writeInt(int)}).
     * <p>
     * Object elements are preceded by a boolean, true if null, false otherwise. The first non-null value for a given
     * Object element is then preceded by the name of the class. All non-null values are then written with a writer
     * method from {@link SerializationUtils#getWriter(Class, ObjectOutput)}, cached in cachedWriters.
     *
     * @param out The output
     * @param cachedWriters The cached writers
     */
    void writeExternalStreaming(@NotNull ObjectOutput out,
            @NotNull TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException;

    /**
     * Implement the Object element writing protocol described in
     * {@link #writeExternalStreaming(ObjectOutput, TIntObjectMap)}.
     *
     * @param out The output
     * @param cachedWriters The cached writers
     * @param itemIndex The index into the cached writers for this item
     * @param item The item to write
     */
    static <ITEM_TYPE> void writeObjectElement(@NotNull final ObjectOutput out,
            @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters,
            final int itemIndex,
            @Nullable ITEM_TYPE item) throws IOException {
        if (item == null) {
            out.writeBoolean(true);
            return;
        }
        out.writeBoolean(false);
        // noinspection unchecked
        SerializationUtils.Writer<ITEM_TYPE> writer = cachedWriters.get(itemIndex);
        if (writer == null) {
            // noinspection unchecked
            final Class<ITEM_TYPE> itemClass = (Class<ITEM_TYPE>) item.getClass();
            out.writeUTF(itemClass.getName());
            cachedWriters.put(itemIndex, writer = SerializationUtils.getWriter(itemClass, out));
        }
        writer.accept(item);
    }

    /**
     * <p>
     * Alternative to {@link java.io.Externalizable#readExternal(ObjectInput)} for use when a series of tuples of the
     * same type with the same element types are being read in iterative fashion.
     * <p>
     * Primitive elements should be read with their primitive read methods (e.g. {@link ObjectInput#readInt()}).
     * <p>
     * Object elements are preceded by a boolean, true if null, false otherwise. The first non-null value for a given
     * Object element is then preceded by the name of the class. All non-null values are then read with a reader method
     * from {@link SerializationUtils#getReader(Class, ObjectInput)}, cached in cachedReaders.
     *
     * @param in The input
     * @param cachedReaders The cached readers
     */
    void readExternalStreaming(@NotNull ObjectInput in, @NotNull TIntObjectMap<SerializationUtils.Reader> cachedReaders)
            throws Exception;

    /**
     * Convenience method to allow chaining of construction and calls to
     * {@link #readExternalStreaming(ObjectInput, TIntObjectMap)}.
     *
     * @param in The input
     * @param cachedReaders The cached readers
     * @return this
     */
    default <TYPE extends StreamingExternalizable> TYPE initializeExternalStreaming(@NotNull final ObjectInput in,
            @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {
        readExternalStreaming(in, cachedReaders);
        // noinspection unchecked
        return (TYPE) this;
    }

    /**
     * Implement the Object element reading protocol described in
     * {@link #readExternalStreaming(ObjectInput, TIntObjectMap)}.
     *
     * @param in The input
     * @param cachedReaders The cached readers
     * @param itemIndex The index into the cached readers for this item
     */
    static <ITEM_TYPE> ITEM_TYPE readObjectElement(@NotNull final ObjectInput in,
            @NotNull TIntObjectMap<SerializationUtils.Reader> cachedReaders,
            final int itemIndex) throws Exception {
        if (in.readBoolean()) {
            return null;
        }
        // noinspection unchecked
        SerializationUtils.Reader<ITEM_TYPE> reader = cachedReaders.get(itemIndex);
        if (reader == null) {
            final String itemClassName = in.readUTF();
            // noinspection unchecked
            final Class<ITEM_TYPE> itemClass = (Class<ITEM_TYPE>) Class.forName(itemClassName);

            cachedReaders.put(itemIndex, reader = SerializationUtils.getReader(itemClass, in));
        }
        return reader.get();
    }
}
