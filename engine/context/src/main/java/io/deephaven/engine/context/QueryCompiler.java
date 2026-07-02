//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;

/**
 * Java compiler api, specifically geared towards query language expressions. Implementations should be constructed with
 * a path on disk to existing bytecode to use in addition to `java.class.path`. Likewise, callers must ensure that the
 * context classloader already contains those input classes - the new class will be loaded in a child classloader to
 * ensure that the class can be GCd when neither it nor the compiler instance are reachable.
 * <p>
 * Requests for compilation will never depend on other classes compiled by this or any other QueryCompiler, and are
 * expected to be unique based on the provided classname. This enables the implementation to cache classes and only
 * compile and load a class once per name.
 */
public interface QueryCompiler {

    /**
     * Compile a class and wait for completion.
     *
     * @param request The compilation request
     */
    @FinalDefault
    default Class<?> compile(@NotNull final QueryCompilerRequest request) {
        final CompletionStageFuture.Resolver<Class<?>> resolver = CompletionStageFuture.make();
        compile(request, resolver);
        try {
            return resolver.getFuture().get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new UncheckedDeephavenException("Error while compiling class", cause);
        } catch (InterruptedException e) {
            throw new UncheckedDeephavenException("Interrupted while compiling class", e);
        }
    }

    /**
     * Compile a class.
     *
     * @param request The compilation request
     * @param resolver The resolver to use for delivering compilation results
     */
    @FinalDefault
    default void compile(
            @NotNull final QueryCompilerRequest request,
            @NotNull final CompletionStageFuture.Resolver<Class<?>> resolver) {
        // noinspection unchecked
        compile(new QueryCompilerRequest[] {request}, new CompletionStageFuture.Resolver[] {resolver});
    }

    /**
     * Compiles all requests.
     *
     * @param requests The compilation requests; these must be independent of each other
     * @param resolvers The resolvers to use for delivering compilation results
     */
    void compile(
            @NotNull QueryCompilerRequest[] requests,
            @NotNull CompletionStageFuture.Resolver<Class<?>>[] resolvers);
}
