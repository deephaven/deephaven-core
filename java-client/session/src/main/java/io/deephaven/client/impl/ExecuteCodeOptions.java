//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
     * If the code should be executed systemically or not. When code is executed systemically and systemic object
     * marking is enabled on the server, objects the script creates are marked as systemic. Update errors in systemic
     * objects are passed to the server's registered error reporter, if there is one.
     *
     * <p>
     * The default value is {@link SystemicType#ServerDefault}, which uses the server's default behavior. See the
     * documentation for SystemicObjectTracker for more details.
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
         * Set if the code should be executed systemically or not. {@link SystemicType#ServerDefault} uses the server's
         * default behavior.
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
