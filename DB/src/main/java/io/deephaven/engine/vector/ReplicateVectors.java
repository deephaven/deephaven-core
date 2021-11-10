/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.vector;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.compilertools.ReplicateUtilities;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicateVectors {

    @SuppressWarnings("AutoBoxing")
    public static void main(String[] args) throws IOException {
        Map<String, Long> serialVersionUIDs = Collections.emptyMap();

        final String charVectorJavaPath = "DB/src/main/java/io/deephaven/engine/tables/dbarrays/CharVector.java";

        ReplicatePrimitiveCode.charToAllButBooleanAndFloats(charVectorJavaPath, serialVersionUIDs);

        final String floatPath = ReplicatePrimitiveCode.charToFloat(charVectorJavaPath, serialVersionUIDs);
        final File floatFile = new File(floatPath);
        List<String> floatLines = FileUtils.readLines(floatFile, Charset.defaultCharset());
        floatLines = ReplicateUtilities.simpleFixup(floatLines, "elementEquals",
                "aArray\\.get\\(ei\\) != bArray\\.get\\(ei\\)",
                "Float.floatToIntBits(aArray.get(ei)) != Float.floatToIntBits(bArray.get(ei))");
        FileUtils.writeLines(floatFile, floatLines);

        final String doublePath = ReplicatePrimitiveCode.charToDouble(charVectorJavaPath, serialVersionUIDs);
        final File doubleFile = new File(doublePath);
        List<String> doubleLines = FileUtils.readLines(doubleFile, Charset.defaultCharset());
        doubleLines = ReplicateUtilities.simpleFixup(doubleLines, "elementEquals",
                "aArray\\.get\\(ei\\) != bArray\\.get\\(ei\\)",
                "Double.doubleToLongBits(aArray.get(ei)) != Double.doubleToLongBits(bArray.get(ei))");
        FileUtils.writeLines(doubleFile, doubleLines);

        ReplicatePrimitiveCode.charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/tables/dbarrays/CharVectorDirect.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/tables/dbarrays/CharVectorSlice.java",
                serialVersionUIDs);
        ReplicatePrimitiveCode.charToAllButBoolean(
                "DB/src/main/java/io/deephaven/engine/tables/dbarrays/CharSubVector.java",
                serialVersionUIDs);

        // Uncomment if running from the IDE:
        // io.deephaven.engine.v2.dbarrays.ReplicateVectorColumnWrappers.main(new String[0]);
    }
}
