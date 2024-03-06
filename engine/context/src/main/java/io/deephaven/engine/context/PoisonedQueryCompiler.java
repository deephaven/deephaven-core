//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.util.ExecutionContextRegistrationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Map;

public class PoisonedQueryCompiler extends QueryCompiler {

    public static final PoisonedQueryCompiler INSTANCE = new PoisonedQueryCompiler();

    private PoisonedQueryCompiler() {}

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("QueryCompiler");
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
