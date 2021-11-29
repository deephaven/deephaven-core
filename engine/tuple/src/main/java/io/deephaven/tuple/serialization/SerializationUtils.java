package io.deephaven.tuple.serialization;

import io.deephaven.time.DateTime;
import io.deephaven.util.FunctionalInterfaces;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.util.Date;

/**
 * Utility class for faster type-specific Object serialization and deserialization.
 */
public class SerializationUtils {

    public interface Writer<ITEM_TYPE> extends FunctionalInterfaces.ThrowingConsumer<ITEM_TYPE, IOException> {
    }

    /**
     * Get a serializing consumer for the supplied item class and output.
     *
     * @param itemClass The item class
     * @param out The output
     * @return A new serializing consumer
     */
    public static <ITEM_TYPE> Writer<ITEM_TYPE> getWriter(@NotNull final Class<ITEM_TYPE> itemClass,
            @NotNull final ObjectOutput out) {
        if (itemClass == Byte.class) {
            return k -> out.writeByte((Byte) k);
        }
        if (itemClass == Short.class) {
            return k -> out.writeShort((Short) k);
        }
        if (itemClass == Integer.class) {
            return k -> out.writeInt((Integer) k);
        }
        if (itemClass == Long.class) {
            return k -> out.writeLong((Long) k);
        }
        if (itemClass == Float.class) {
            return k -> out.writeFloat((Float) k);
        }
        if (itemClass == Double.class) {
            return k -> out.writeDouble((Double) k);
        }
        if (itemClass == Boolean.class) {
            return k -> out.writeBoolean((Boolean) k);
        }
        if (itemClass == Character.class) {
            return k -> out.writeChar((Character) k);
        }
        if (itemClass == String.class) {
            return k -> out.writeUTF((String) k);
        }
        if (itemClass == DateTime.class) {
            return k -> out.writeLong(((DateTime) k).getNanos());
        }
        if (itemClass == Date.class) {
            return k -> out.writeLong(((Date) k).getTime());
        }
        if (StreamingExternalizable.class.isAssignableFrom(itemClass)) {
            final TIntObjectMap<Writer> cachedWriters = new TIntObjectHashMap<>();
            return k -> ((StreamingExternalizable) k).writeExternalStreaming(out, cachedWriters);
        }
        if (Externalizable.class.isAssignableFrom(itemClass)) {
            return k -> ((Externalizable) k).writeExternal(out);
        }
        return out::writeObject;
    }

    public interface Reader<ITEM_TYPE> extends FunctionalInterfaces.ThrowingSupplier<ITEM_TYPE, Exception> {
    }

    /**
     * Get a deserializing supplier for the supplied item class and input.
     *
     * @param itemClass The item class
     * @param in The input
     * @return A new deserializing supplier
     */
    @SuppressWarnings("unchecked")
    public static <ITEM_TYPE> Reader<ITEM_TYPE> getReader(@NotNull final Class<ITEM_TYPE> itemClass,
            @NotNull final ObjectInput in) {
        if (itemClass == Byte.class) {
            return () -> (ITEM_TYPE) Byte.valueOf(in.readByte());
        }
        if (itemClass == Short.class) {
            return () -> (ITEM_TYPE) Short.valueOf(in.readShort());
        }
        if (itemClass == Integer.class) {
            return () -> (ITEM_TYPE) Integer.valueOf(in.readInt());
        }
        if (itemClass == Long.class) {
            return () -> (ITEM_TYPE) Long.valueOf(in.readLong());
        }
        if (itemClass == Float.class) {
            return () -> (ITEM_TYPE) Float.valueOf(in.readFloat());
        }
        if (itemClass == Double.class) {
            return () -> (ITEM_TYPE) Double.valueOf(in.readDouble());
        }
        if (itemClass == Boolean.class) {
            return () -> (ITEM_TYPE) Boolean.valueOf(in.readBoolean());
        }
        if (itemClass == Character.class) {
            return () -> (ITEM_TYPE) Character.valueOf(in.readChar());
        }
        if (itemClass == String.class) {
            return () -> (ITEM_TYPE) in.readUTF();
        }
        if (itemClass == DateTime.class) {
            return () -> (ITEM_TYPE) new DateTime(in.readLong());
        }
        if (itemClass == Date.class) {
            return () -> (ITEM_TYPE) new Date(in.readLong());
        }
        if (StreamingExternalizable.class.isAssignableFrom(itemClass)) {
            final Constructor constructor;
            try {
                constructor = itemClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new UnsupportedOperationException("Can't deserialize keys of type " + itemClass
                        + ", could not get no-arg constructor for StreamingExternalizable type");
            }
            final TIntObjectMap<Reader> cachedReaders = new TIntObjectHashMap<>();
            return () -> {
                final StreamingExternalizable key = (StreamingExternalizable) constructor.newInstance();
                key.readExternalStreaming(in, cachedReaders);
                return (ITEM_TYPE) key;
            };
        }
        if (Externalizable.class.isAssignableFrom(itemClass)) {
            final Constructor constructor;
            try {
                constructor = itemClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new UnsupportedOperationException("Can't deserialize keys of type " + itemClass
                        + ", could not get no-arg constructor for Externalizable type");
            }
            return () -> {
                final Externalizable key = (Externalizable) constructor.newInstance();
                key.readExternal(in);
                return (ITEM_TYPE) key;
            };
        }
        return () -> (ITEM_TYPE) in.readObject();
    }
}
