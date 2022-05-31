package io.deephaven.engine.util.scripts;

import java.util.*;

/**
 * Maps from script language name to file extensions.
 */
public class ScriptExtensionsMap extends HashMap<String, List<String>> {
    private ScriptExtensionsMap() {
        put("Groovy", Collections.singletonList("groovy"));
        put("Python", Collections.singletonList("py"));
        put("Scala", Collections.singletonList("scala"));
        put("JavaScript", Arrays.asList("js", "javascript"));
    }

    private static Map<String, List<String>> instance;

    synchronized public static Map<String, List<String>> getInstance() {
        if (instance == null) {
            instance = Collections.unmodifiableMap(new ScriptExtensionsMap());
        }
        return instance;
    }
}
