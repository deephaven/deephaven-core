//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * An object to control the behavior of the {@link ConsoleSession#executeCode(String, ExecuteCodeOptions) executeCode}
 * API
 */
@Value.Immutable
@BuildableStyle
public interface ExecuteCodeOptions {
    /**
     * The default options. See the method javadoc for default values.
     */
    ExecuteCodeOptions DEFAULT = ExecuteCodeOptions.builder().build();

    enum SystemicType {
        ServerDefault, Systemic, NotSystemic
    }

    /**
     * If the code should be executed systemically or not. When code is executed systemically, failures of the script or
     * tables in the script are fatal.
     *
     * <p>
     * The default value is {@code null} which uses the system default behavior. See the documentation for
     * SystemicObjectTracker for more details.
     * </p>
     * 
     * @return if the code should be systemically executed.
     */
    @Value.Default
    default SystemicType executeSystemic() {
        return SystemicType.ServerDefault;
    }

    /**
     * Create a new options builder.
     *
     * @return a new builder
     */
    static Builder builder() {
        return ImmutableExecuteCodeOptions.builder();
    }

    interface Builder {
        /**
         * Set if the code should be executed systemically or not. A value of {@code null} uses default system behavior.
         *
         * @param systemicType if the code should be executed systemically.
         * @return this {@link Builder}
         */
        ExecuteCodeOptions.Builder executeSystemic(SystemicType systemicType);

        /**
         * Create a new {@link ExecuteCodeOptions} from the state of this builder.
         * 
         * @return a new {@link ExecuteCodeOptions}
         */
        ExecuteCodeOptions build();
    }
}
