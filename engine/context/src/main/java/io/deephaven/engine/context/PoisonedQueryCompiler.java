//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.ExecutionContextRegistrationException;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public class PoisonedQueryCompiler implements QueryCompiler {

    public static final PoisonedQueryCompiler INSTANCE = new PoisonedQueryCompiler();

    private PoisonedQueryCompiler() {}

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("QueryCompiler");
    }

    @Override
    public File getTemporaryClassDestination() {
        return fail();
    }

    @Override
    public void setParentClassLoader(@NotNull final ClassLoader parentClassLoader) {
        fail();
    }

    @Override
    public void compile(
            @NotNull final QueryCompilerRequest[] requests,
            @NotNull final CompletionStageFuture.Resolver<Class<?>>[] resolvers) {
        fail();
    }
}
