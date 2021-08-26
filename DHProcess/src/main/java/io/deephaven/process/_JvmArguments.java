package io.deephaven.process;

import java.lang.management.RuntimeMXBean;
import org.immutables.value.Value;

/**
 * Represents the JVM input arguments as collected via {@link RuntimeMXBean#getInputArguments()}.
 */
@Value.Immutable
@Wrapped
abstract class _JvmArguments extends StringListWrapper {

    static JvmArguments of(RuntimeMXBean bean) {
        return JvmArguments.of(bean.getInputArguments());
    }
}
