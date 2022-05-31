package io.deephaven.benchmark.engine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Utils {
    static final Map<String, Class> primitiveTypeForName;
    static {
        Map<String, Class> primitiveTypeForNameLocal = new HashMap<>();
        primitiveTypeForNameLocal.put("byte", byte.class);
        primitiveTypeForNameLocal.put("boolean", boolean.class);
        primitiveTypeForNameLocal.put("char", char.class);
        primitiveTypeForNameLocal.put("double", double.class);
        primitiveTypeForNameLocal.put("int", int.class);
        primitiveTypeForNameLocal.put("float", float.class);
        primitiveTypeForNameLocal.put("long", long.class);
        primitiveTypeForNameLocal.put("short", short.class);
        primitiveTypeForName = Collections.unmodifiableMap(primitiveTypeForNameLocal);
    }


}
