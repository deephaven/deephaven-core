/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegistered;

import java.io.File;
import java.util.Hashtable;
import java.util.Map;

public class PoisonedCompilerToolsContext implements CompilerTools.Context {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedCompilerToolsContext.class);

    @Override
    public Hashtable<String, CompilerTools.SimplePromise<Class<?>>> getKnownClasses() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public ClassLoader getClassLoaderForFormula(Map<String, Class<?>> parameterClasses) {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public File getClassDestination() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public String getClassPath() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public File getFakeClassDestination() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public void setParentClassLoader(ClassLoader parentClassLoader) {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegistered();
    }
}
