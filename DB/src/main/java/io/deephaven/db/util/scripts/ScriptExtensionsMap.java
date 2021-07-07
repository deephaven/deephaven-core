package io.deephaven.db.util.scripts;

import io.deephaven.lang.parse.api.Languages;

import java.util.*;

/**
 *  Maps from script language name to file extensions.
 */
public class ScriptExtensionsMap extends HashMap<String, List<String>> {
    private ScriptExtensionsMap() {
        put(Languages.LANGUAGE_GROOVY, Collections.singletonList("groovy"));
        put(Languages.LANGUAGE_PYTHON, Collections.singletonList("py"));
        put(Languages.LANGUAGE_SCALA, Collections.singletonList("scala"));
        put(Languages.LANGUAGE_JAVASCRIPT, Arrays.asList("js", "javascript"));
    }

    private static Map<String,List<String>> instance;
    synchronized public static Map<String,List<String>> getInstance() {
        if (instance == null) {
            instance = Collections.unmodifiableMap(new ScriptExtensionsMap());
        }
        return instance;
    }
}
