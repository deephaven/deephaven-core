/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.replicators;

import io.deephaven.replication.ReplicatePrimitiveCode;
import io.deephaven.replication.ReplicationUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.deephaven.replication.ReplicatePrimitiveCode.*;
import static io.deephaven.replication.ReplicationUtils.*;
import static io.deephaven.replication.ReplicationUtils.replaceRegion;

public class ReplicateRingBuffers {

    public static void main(String... args) throws IOException {
        // replicate ring buffers to all but Object (since RingBuffer<> already exisits)
        charToAllButBoolean("Base/src/main/java/io/deephaven/base/ringbuffer/CharRingBuffer.java");
        String objectResult = ReplicatePrimitiveCode.charToObject(
                "Base/src/main/java/io/deephaven/base/ringbuffer/CharRingBuffer.java");
        File objectFile = new File(objectResult);
        List<String> lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

        lines = ReplicationUtils.addImport(lines, "import java.util.Arrays;");

        lines = ReplicationUtils.globalReplacements(lines,
                "class ObjectRingBuffer", "class ObjectRingBuffer<T>",
                "Object e", "T e",
                "public Object ", "public T ",
                "Object onEmpty", "T onEmpty",
                "Object notFullResult", "T notFullResult",
                "return storage\\[", "return (T)storage[",
                "Object val = notFullResult", "T val = notFullResult",
                "Object val = storage", "T val = (T)storage");
        lines = ReplicationUtils.replaceRegion(lines, "object-remove",
                Collections.singletonList("        storage[idx] = null;"));

        lines = ReplicationUtils.replaceRegion(lines, "object-bulk-remove",
                Collections.singletonList(
                        "        final int storageHead = (int) (head & mask);\n" +
                                "\n" +
                                "        // firstCopyLen is either the size of the ring buffer, the distance from head to the end of the storage array,\n"
                                +
                                "        // or the size of the destination buffer, whichever is smallest.\n" +
                                "        final int firstCopyLen = Math.min(Math.min(storage.length - storageHead, size), result.length);\n"
                                +
                                "\n" +
                                "        // secondCopyLen is either the number of uncopied elements remaining from the first copy,\n"
                                +
                                "        // or the amount of space remaining in the dest array, whichever is smaller.\n"
                                +
                                "        final int secondCopyLen = Math.min(size - firstCopyLen, result.length - firstCopyLen);\n"
                                +
                                "\n" +
                                "        System.arraycopy(storage, storageHead, result, 0, firstCopyLen);\n" +
                                "        Arrays.fill(storage, storageHead, storageHead + firstCopyLen, null);\n" +
                                "        System.arraycopy(storage, 0, result, firstCopyLen, secondCopyLen);\n" +
                                "        Arrays.fill(storage, 0, secondCopyLen, null);" +
                                "\n"));
        FileUtils.writeLines(objectFile, lines);

        charToAllButBoolean(
                "Base/src/main/java/io/deephaven/base/ringbuffer/AggregatingCharRingBuffer.java");
        objectResult = ReplicatePrimitiveCode.charToObject(
                "Base/src/main/java/io/deephaven/base/ringbuffer/AggregatingCharRingBuffer.java");
        objectFile = new File(objectResult);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "Object identityVal", "T identityVal",
                "public Object removeUnsafe", "public T removeUnsafe",
                "return val;", "return (T)val;",
                "public interface ObjectFunction \\{", "public interface ObjectFunction<T> {",
                "Object apply\\(Object a, Object b\\);", "T apply(T a, T b);",
                "public Object evaluate", "public T evaluate",
                "ObjectFunction aggFunction", "ObjectFunction<T> aggFunction",
                "return treeStorage\\[1\\]", "return (T)treeStorage[1]",
                "class AggregatingObjectRingBuffer extends ObjectRingBuffer",
                "class AggregatingObjectRingBuffer<T> extends ObjectRingBuffer<T>");
        FileUtils.writeLines(objectFile, lines);


        // replicate the tests
        charToAllButBoolean("Base/src/test/java/io/deephaven/base/ringbuffer/CharRingBufferTest.java");
        objectResult = ReplicatePrimitiveCode.charToObject(
                "Base/src/test/java/io/deephaven/base/ringbuffer/CharRingBufferTest.java");
        objectFile = new File(objectResult);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "Object.MIN_VALUE", "new Object()");
        FileUtils.writeLines(objectFile, lines);

        List<String> files = charToAllButBoolean(
                "Base/src/test/java/io/deephaven/base/ringbuffer/AggregatingCharRingBufferTest.java");
        for (final String f : files) {
            if (f.contains("Byte")) {
                objectFile = new File(f);
                lines = FileUtils.readLines(objectFile, Charset.defaultCharset());

                lines = replaceRegion(lines, "non-byte-tests",
                        Collections.singletonList("    // Tests removed due to limitations of byte storage"));
                FileUtils.writeLines(objectFile, lines);
            }
        }
    }
}
