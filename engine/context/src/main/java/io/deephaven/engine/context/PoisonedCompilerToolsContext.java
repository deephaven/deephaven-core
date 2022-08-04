/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegisteredException;

import java.io.File;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PoisonedCompilerToolsContext implements CompilerTools.Context {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedCompilerToolsContext.class);
    public static final PoisonedCompilerToolsContext INSTANCE = new PoisonedCompilerToolsContext();

    @Override
    public Hashtable<String, CompletableFuture<Class<?>>> getKnownClasses() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public ClassLoader getClassLoaderForFormula(Map<String, Class<?>> parameterClasses) {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public File getClassDestination() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public String getClassPath() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public File getFakeClassDestination() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public void setParentClassLoader(ClassLoader parentClassLoader) {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }
}
