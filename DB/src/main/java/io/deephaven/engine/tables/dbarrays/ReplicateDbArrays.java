/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.dbarrays;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicateDbArrays {

    @SuppressWarnings("AutoBoxing")
    public static void main(String[] args) throws IOException {
        Map<String, Long> serialVersionUIDs = new HashMap<>();
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbArrayBase", -2429677814745466454L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbArrayDirect", 9111886364211462917L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbBooleanArrayDirect", -9116229390345474761L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbCharArray", -1373264425081841175L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbLongArray", -4934601086974582202L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbIntArray", -4282375411744560278L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbShortArray", -6562228894877343013L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbByteArray", 8519130615638683196L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbDoubleArray", 7218901311693729986L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbFloatArray", -1889118072737983807L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbCharArrayDirect", 3636374971797603565L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbLongArrayDirect", 1233975234000551534L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbIntArrayDirect", -7790095389322728763L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbShortArrayDirect", -4415134364550246624L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbByteArrayDirect", 5978679490703697461L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbDoubleArrayDirect", 3262776153086160765L);
        serialVersionUIDs.put("io.deephaven.engine.tables.dbarrays.DbFloatArrayDirect", -8263599481663466384L);

        final String dbCharArrayJavaPath = "DB/src/main/java/io/deephaven/engine/tables/dbarrays/DbCharArray.java";

        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(dbCharArrayJavaPath, serialVersionUIDs);

        final String floatPath = ReplicatePrimitiveCode.charToFloat(dbCharArrayJavaPath, serialVersionUIDs);
        final File floatFile = new File(floatPath);
        List<String> floatLines = FileUtils.readLines(floatFile, Charset.defaultCharset());
        floatLines = ReplicateUtilities.simpleFixup(floatLines, "elementEquals",
                "aArray\\.get\\(ei\\) != bArray\\.get\\(ei\\)",
                "Float.floatToIntBits(aArray.get(ei)) != Float.floatToIntBits(bArray.get(ei))");
        FileUtils.writeLines(floatFile, floatLines);

        final String doublePath = ReplicatePrimitiveCode.charToDouble(dbCharArrayJavaPath, serialVersionUIDs);
        final File doubleFile = new File(doublePath);
        List<String> doubleLines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        doubleLines = ReplicateUtilities.simpleFixup(doubleLines, "elementEquals",
                "aArray\\.get\\(ei\\) != bArray\\.get\\(ei\\)",
                "Double.doubleToLongBits(aArray.get(ei)) != Double.doubleToLongBits(bArray.get(ei))");
        FileUtils.writeLines(doubleFile, doubleLines);

        ReplicatePrimitiveCode.charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/tables/dbarrays/DbCharArrayDirect.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/tables/dbarrays/DbCharArraySlice.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/tables/dbarrays/DbSubCharArray.java",
                serialVersionUIDs);

        // Uncomment if running from the IDE:
        // io.deephaven.engine.v2.dbarrays.ReplicateDbArrayColumnWrappers.main(new String[0]);
    }
}
