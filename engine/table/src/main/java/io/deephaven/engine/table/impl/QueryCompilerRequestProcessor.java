//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.util.MultiException;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.datastructures.CachingSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface QueryCompilerRequestProcessor {

    /**
     * @return An immediate QueryCompilerRequestProcessor
     */
    static QueryCompilerRequestProcessor.ImmediateProcessor immediate() {
        return new ImmediateProcessor();
    }

    /**
     * @return A batch QueryCompilerRequestProcessor
     */
    static QueryCompilerRequestProcessor.BatchProcessor batch() {
        return new BatchProcessor();
    }

    /**
     * @return a CachingSupplier that supplies a snapshot of the current query scope variables
     */
    @VisibleForTesting
    static CachingSupplier<Map<String, Object>> newQueryScopeVariableSupplier() {
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        return new CachingSupplier<>(() -> Collections.unmodifiableMap(
                queryScope.toMap((name, value) -> NameValidator.isValidQueryParameterName(name))));
    }

    /**
     * @return a lazily cached snapshot of the current query scope variables
     */
    Map<String, Object> getQueryScopeVariables();

    /**
     * Submit a request for compilation. The QueryCompilerRequestProcessor is not required to immediately compile this
     * request.
     *
     * @param request the request to compile
     */
    CompletionStageFuture<Class<?>> submit(@NotNull QueryCompilerRequest request);

    /**
     * A QueryCompilerRequestProcessor that immediately compiles requests.
     */
    class ImmediateProcessor implements QueryCompilerRequestProcessor {

        private final CachingSupplier<Map<String, Object>> queryScopeVariableSupplier = newQueryScopeVariableSupplier();

        private ImmediateProcessor() {
            // force use of static factory method
        }

        @Override
        public Map<String, Object> getQueryScopeVariables() {
            return queryScopeVariableSupplier.get();
        }

        @Override
        public CompletionStageFuture<Class<?>> submit(@NotNull final QueryCompilerRequest request) {
            final String desc = "Compile: " + request.description();
            final CompletionStageFuture.Resolver<Class<?>> resolver = CompletionStageFuture.make();
            try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getCompilationNugget(desc)) {
                ExecutionContext.getContext().getQueryCompiler().compile(request, resolver);
            }

            // The earlier we validate the future, the better.
            final CompletionStageFuture<Class<?>> future = resolver.getFuture();
            try {
                future.get(0, TimeUnit.SECONDS);
            } catch (ExecutionException err) {
                throw new UncheckedDeephavenException("Compilation failed", err.getCause());
            } catch (InterruptedException | TimeoutException err) {
                // This should never happen since the future is already completed.
                throw new UncheckedDeephavenException("Caught unexpected exception", err);
            }

            return future;
        }
    }

    /**
     * A QueryCompilerRequestProcessor that batches requests and compiles them all at once.
     * <p>
     * The compile method must be called to actually compile the requests.
     */
    class BatchProcessor implements QueryCompilerRequestProcessor {
        private final List<QueryCompilerRequest> requests = new ArrayList<>();
        private final List<CompletionStageFuture.Resolver<Class<?>>> resolvers = new ArrayList<>();
        private final CachingSupplier<Map<String, Object>> queryScopeVariableSupplier = newQueryScopeVariableSupplier();

        private BatchProcessor() {
            // force use of static factory method
        }

        @Override
        public Map<String, Object> getQueryScopeVariables() {
            return queryScopeVariableSupplier.get();
        }

        @Override
        public CompletionStageFuture<Class<?>> submit(@NotNull final QueryCompilerRequest request) {
            final CompletionStageFuture.Resolver<Class<?>> resolver = CompletionStageFuture.make();
            requests.add(request);
            resolvers.add(resolver);
            return resolver.getFuture();
        }

        /**
         * Compile all the requests that have been submitted.
         */
        public void compile() {
            if (requests.isEmpty()) {
                return;
            }

            final String desc;
            if (requests.size() == 1) {
                desc = "Compile: " + requests.get(0).description();
            } else {
                final StringBuilder descriptionBuilder = new StringBuilder();
                descriptionBuilder.append("Batch Compile of ").append(requests.size()).append(" requests:\n");
                for (final QueryCompilerRequest request : requests) {
                    descriptionBuilder.append('\t').append(request.description()).append('\n');
                }
                desc = descriptionBuilder.toString();
            }

            try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getCompilationNugget(desc)) {
                final QueryCompiler compiler = ExecutionContext.getContext().getQueryCompiler();
                // noinspection unchecked
                compiler.compile(
                        requests.toArray(QueryCompilerRequest[]::new),
                        resolvers.toArray(CompletionStageFuture.Resolver[]::new));

                final List<Throwable> exceptions = new ArrayList<>();
                for (CompletionStageFuture.Resolver<Class<?>> resolver : resolvers) {
                    try {
                        Object ignored2 = resolver.getFuture().get();
                    } catch (ExecutionException err) {
                        exceptions.add(err.getCause());
                    } catch (InterruptedException err) {
                        exceptions.add(err);
                    }
                }
                if (!exceptions.isEmpty()) {
                    throw new UncheckedDeephavenException("Compilation failed",
                            MultiException.maybeWrapInMultiException("Compilation exceptions for multiple requests",
                                    exceptions));
                }
            }
        }
    }
}
