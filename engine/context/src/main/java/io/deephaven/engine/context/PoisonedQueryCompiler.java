/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegisteredException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Map;

public class PoisonedQueryCompiler extends QueryCompiler {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedQueryCompiler.class);
    public static final PoisonedQueryCompiler INSTANCE = new PoisonedQueryCompiler();

    private PoisonedQueryCompiler() {}

    private <T> T fail() {
        logger.error().append("No ExecutionContext provided; cannot use QueryCompiler").endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public File getFakeClassDestination() {
        return fail();
    }

    @Override
    public void setParentClassLoader(ClassLoader parentClassLoader) {
        fail();
    }

    @Override
    public Class<?> compile(@NotNull String className, @NotNull String classBody, @NotNull String packageNameRoot,
            @Nullable StringBuilder codeLog, @NotNull Map<String, Class<?>> parameterClasses) {
        return fail();
    }
}
