//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;

import java.io.PrintStream;

/**
 * Provides a structured approach for constructing {@link DeephavenApiServerComponent}.
 *
 * @param <Component> the component type
 */
public abstract class ComponentFactoryBase<Component extends DeephavenApiServerComponent> {

    /**
     * Constructs a component according to {@code configuration} and using the prints streams from
     * {@link PrintStreamGlobals}.
     *
     * <p>
     * Equivalent to {@code build(configuration, PrintStreamGlobals.getOut(), PrintStreamGlobals.getErr())}.
     *
     * @param configuration the configuration
     * @return the component
     * @see #build(Configuration, PrintStream, PrintStream)
     * @see PrintStreamGlobals#getOut()
     * @see PrintStreamGlobals#getErr()
     */
    public final Component build(Configuration configuration) {
        return build(configuration, PrintStreamGlobals.getOut(), PrintStreamGlobals.getErr());
    }

    /**
     * Constructs a component according to the {@code configuration}, invoking
     * {@link io.deephaven.server.runner.DeephavenApiServerComponent.Builder#withOut(PrintStream)} with {@code out} and
     * {@link io.deephaven.server.runner.DeephavenApiServerComponent.Builder#withErr(PrintStream)} with {@code err}.
     *
     * @param configuration the configuration
     * @param out the out
     * @param err the err
     * @return the component
     */
    public abstract Component build(Configuration configuration, PrintStream out, PrintStream err);
}
