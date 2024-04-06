//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
