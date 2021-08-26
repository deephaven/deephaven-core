package io.deephaven.parquet;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

public class Replicate {

    public static void main(String[] args) throws IOException {

        ReplicatePrimitiveCode.intToLongAndFloatingPoints(PlainIntChunkedWriter.class, ReplicatePrimitiveCode.MAIN_SRC,
                "int pageSize", "IntBuffer.allocate(4)", "int originalLimit", "int writeBulk", "int valueCount",
                "int rowCount",
                "int nullCount", "writeInt\\(", "IntBuffer repeatCount", "length != Integer.MIN_VALUE", "int length",
                "int i = 0;", "int targetCapacity", "IntBuffer nullOffsets");
    }

}
