package io.deephaven.process;

import java.lang.management.RuntimeMXBean;
import org.immutables.value.Value;

/**
 * Represents the system properties as collected via {@link RuntimeMXBean#getSystemProperties()}.
 */
@Value.Immutable
@Wrapped
abstract class _SystemProperties extends StringMapWrapper {

    static SystemProperties of(RuntimeMXBean bean) {
        return SystemProperties.of(bean.getSystemProperties());
    }
}
