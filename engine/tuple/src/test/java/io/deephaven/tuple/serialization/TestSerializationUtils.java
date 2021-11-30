package io.deephaven.tuple.serialization;

import io.deephaven.time.DateTime;
import io.deephaven.tuple.ArrayTuple;
import io.deephaven.tuple.generated.ObjectObjectObjectTuple;
import io.deephaven.tuple.generated.ObjectObjectTuple;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.*;
import java.util.Date;

/**
 * Test serialization code used for {@link StreamingExternalizable} implementations.
 */
public class TestSerializationUtils {

    public static class EE implements Externalizable {

        private static final long serialVersionUID = 1L;

        private int member;

        private EE(final int member) {
            this.member = member;
        }

        public EE() {}

        @Override
        public boolean equals(final Object other) {
            if (this == other)
                return true;
            if (other == null || getClass() != other.getClass())
                return false;
            final EE otherEE = (EE) other;
            return member == otherEE.member;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(member);
        }

        @Override
        public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
            out.writeInt(member);
        }

        @Override
        public void readExternal(@NotNull final ObjectInput in) throws IOException {
            member = in.readInt();
        }
    }

    private static class SE implements Serializable {

        private static final long serialVersionUID = 1L;

        private int member;

        private SE(final int member) {
            this.member = member;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other)
                return true;
            if (other == null || getClass() != other.getClass())
                return false;
            final SE otherSE = (SE) other;
            return member == otherSE.member;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(member);
        }

        private void writeObject(@NotNull final ObjectOutputStream out) throws IOException {
            out.writeInt(member);
        }

        private void readObject(@NotNull final ObjectInputStream in) throws IOException {
            member = in.readInt();
        }
    }

    @Test
    public void testAllTypes() throws Exception {
        // noinspection AutoBoxing
        final ArrayTuple fullInput = new ArrayTuple((byte) 1, (short) 2, 3, 4L, 5.0F, 6.0D, true, '7', "08",
                new DateTime(9), new Date(10),
                new ObjectObjectTuple("11-A", "11-B"),
                new ObjectObjectObjectTuple("12-X", "12-Y", "12-Z"),
                new EE(13), new SE(14));
        final ArrayTuple nullInput =
                new ArrayTuple(null, null, null, null, null, null, null, null, null, null, null, null, null, null);

        final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        final DataOutputStream dataOut = new DataOutputStream(bytesOut);
        final ObjectOutputStream objectOut = new ObjectOutputStream(dataOut);

        final TIntObjectMap<SerializationUtils.Writer> cachedWriters = new TIntObjectHashMap<>();
        nullInput.writeExternalStreaming(objectOut, cachedWriters);
        fullInput.writeExternalStreaming(objectOut, cachedWriters);
        nullInput.writeExternalStreaming(objectOut, cachedWriters);
        fullInput.writeExternalStreaming(objectOut, cachedWriters);

        objectOut.flush();
        dataOut.flush();
        bytesOut.flush();
        final byte[] serialized = bytesOut.toByteArray();

        final ByteArrayInputStream bytesIn = new ByteArrayInputStream(serialized);
        final DataInputStream dataIn = new DataInputStream(bytesIn);
        final ObjectInputStream objectIn = new ObjectInputStream(dataIn);

        final TIntObjectMap<SerializationUtils.Reader> cachedReaders = new TIntObjectHashMap<>();
        TestCase.assertEquals(nullInput, new ArrayTuple().initializeExternalStreaming(objectIn, cachedReaders));
        TestCase.assertEquals(fullInput, new ArrayTuple().initializeExternalStreaming(objectIn, cachedReaders));
        TestCase.assertEquals(nullInput, new ArrayTuple().initializeExternalStreaming(objectIn, cachedReaders));
        TestCase.assertEquals(fullInput, new ArrayTuple().initializeExternalStreaming(objectIn, cachedReaders));
    }
}
