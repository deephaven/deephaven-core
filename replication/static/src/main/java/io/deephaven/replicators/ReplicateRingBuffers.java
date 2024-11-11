//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

public class ReplicateRingBuffers {
    private static final String TASK = "replicateRingBuffers";

    public static void main(String... args) throws IOException {
        // replicate ring buffers to all but Object (since RingBuffer<> already exisits)
        charToAllButBoolean(TASK, "Base/src/main/java/io/deephaven/base/ringbuffer/CharRingBuffer.java");
        String objectResult = ReplicatePrimitiveCode.charToObject(TASK,
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
                "Object\\[\\]", "T[]",
                "new Object\\[", "(T[]) new Object[",
                "Object val = storage\\[", "T val = storage[",
                "Object val = notFullResult", "T val = notFullResult");

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

        lines = ReplicationUtils.replaceRegion(lines, "object-bulk-clear",
                Collections.singletonList(
                        "        final int storageHead = (int) (head & mask);\n" +
                                "        final int size = size();\n" +
                                "        // firstLen is either the size of the ring buffer or the distance from head to the end of the storage array.\n"
                                +
                                "        final int firstLen = Math.min(storage.length - storageHead, size);\n" +
                                "        // secondLen is the number of elements remaining from the first clear.\n" +
                                "        final int secondLen = size - firstLen;\n" +
                                "        Arrays.fill(storage, storageHead, storageHead + firstLen, null);\n" +
                                "        Arrays.fill(storage, 0, secondLen, null);"));

        FileUtils.writeLines(objectFile, lines);

        charToAllButBoolean(TASK,
                "Base/src/main/java/io/deephaven/base/ringbuffer/AggregatingCharRingBuffer.java");
        objectResult = ReplicatePrimitiveCode.charToObject(TASK,
                "Base/src/main/java/io/deephaven/base/ringbuffer/AggregatingCharRingBuffer.java");
        objectFile = new File(objectResult);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "Object identityVal", "T identityVal",
                "public Object ", "public T ",
                "public interface ObjectFunction \\{", "public interface ObjectFunction<T> {",
                "Object apply\\(Object a, Object b\\);", "T apply(T a, T b);",
                "public Object evaluate", "public T evaluate",
                "ObjectFunction aggFunction", "ObjectFunction<T> aggFunction",
                "ObjectFunction aggTreeFunction", "ObjectFunction<T> aggTreeFunction",
                "ObjectFunction aggInitialFunction", "ObjectFunction<T> aggInitialFunction",
                "private Object\\[\\] treeStorage", "private T[] treeStorage",
                "new Object\\[", "(T[]) new Object[",
                "Object\\[\\]", "T[]",
                "Object e", "T e",
                "Object val = storage\\[", "T val = storage[",
                "public Object\\[\\] remove\\(", "public T[] remove(",
                "final Object leftVal", "final T leftVal",
                "final Object rightVal", "final T rightVal",
                "final Object computeVal", "final T computeVal",
                "Object val", "T val",
                "Object notFullResult", "T notFullResult",
                "Object onEmpty", "T onEmpty",
                "ObjectFunction evalFunction", "ObjectFunction<T> evalFunction",
                "private static Object defaultValueForThisType", "private T defaultValueForThisType",
                "final ObjectRingBuffer internalBuffer", "final ObjectRingBuffer<T> internalBuffer",
                "internalBuffer = new ObjectRingBuffer\\(", "internalBuffer = new ObjectRingBuffer<>(",
                "class AggregatingObjectRingBuffer", "class AggregatingObjectRingBuffer<T>");
        FileUtils.writeLines(objectFile, lines);


        // replicate the tests
        charToAllButBoolean(TASK, "Base/src/test/java/io/deephaven/base/ringbuffer/TestCharRingBuffer.java");
        objectResult = ReplicatePrimitiveCode.charToObject(TASK,
                "Base/src/test/java/io/deephaven/base/ringbuffer/TestCharRingBuffer.java");
        objectFile = new File(objectResult);
        lines = FileUtils.readLines(objectFile, Charset.defaultCharset());
        lines = ReplicationUtils.globalReplacements(lines,
                "Object.MIN_VALUE", "new Object()");

        lines = ReplicationUtils.replaceRegion(lines, "empty-test",
                Collections.singletonList(
                        "        for (Object val : rb.getStorage()) {\n" +
                                "            assertNull(val);\n" +
                                "        }"));

        FileUtils.writeLines(objectFile, lines);

        List<String> files = charToAllButBoolean(TASK,
                "Base/src/test/java/io/deephaven/base/ringbuffer/TestAggregatingCharRingBuffer.java");
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
