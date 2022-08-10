/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegisteredException;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PoisonedCompilerToolsContext implements CompilerTools.Context {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedCompilerToolsContext.class);
    public static final PoisonedCompilerToolsContext INSTANCE = new PoisonedCompilerToolsContext();

    private PoisonedCompilerToolsContext() {}

    private <T> T fail() {
        logger.error().append("No ExecutionContext provided; cannot use CompilerTools").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public Map<String, CompletableFuture<Class<?>>> getKnownClasses() {
        return fail();
    }

    @Override
    public ClassLoader getClassLoaderForFormula(Map<String, Class<?>> parameterClasses) {
        return fail();
    }

    @Override
    public File getClassDestination() {
        return fail();
    }

    @Override
    public String getClassPath() {
        return fail();
    }

    @Override
    public File getFakeClassDestination() {
        return fail();
    }

    @Override
    public void setParentClassLoader(ClassLoader parentClassLoader) {
        fail();
    }
}
