package io.deephaven.streampublisher;

import java.util.Map;
import java.util.function.Function;

public class KeyOrValueIngestData {
    public static final int NULL_COLUMN_INDEX = -1;

    public Map<String, String> fieldPathToColumnName;
    public int simpleColumnIndex = NULL_COLUMN_INDEX;
    public Function<Object, Object> toObjectChunkMapper = Function.identity();
    public Object extra;
}
