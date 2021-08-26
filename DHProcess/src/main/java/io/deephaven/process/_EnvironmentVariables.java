package io.deephaven.process;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.immutables.value.Value;

/**
 * Represents the loggable environment variables as collected via {@link System#getenv()}.
 */
@Value.Immutable
@Wrapped
abstract class _EnvironmentVariables extends StringMapWrapper {

    private static final String[] PREFIXES = {"PQ_"};

    static EnvironmentVariables of() {
        final Map<String, String> env = System.getenv();
        final Map<String, String> toLog = new LinkedHashMap<>();
        for (Entry<String, String> entry : env.entrySet()) {
            if (!matchesPrefix(entry.getKey())) {
                continue;
            }
            toLog.put(entry.getKey(), entry.getValue());
        }

        return EnvironmentVariables.of(toLog);
    }

    private static boolean matchesPrefix(String key) {
        for (String prefix : PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
