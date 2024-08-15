//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.context.QueryLibrary;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.util.MultiException;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.datastructures.CachingSupplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class QueryCompilerRequestProcessor {

    /**
     * @return An immediate QueryCompilerRequestProcessor
     */
    public static QueryCompilerRequestProcessor.ImmediateProcessor immediate() {
        return new ImmediateProcessor();
    }

    /**
     * @return A batch QueryCompilerRequestProcessor
     */
    public static QueryCompilerRequestProcessor.BatchProcessor batch() {
        return new BatchProcessor();
    }

    /**
     * @return a CachingSupplier that supplies a snapshot of the current query scope variables
     */
    @VisibleForTesting
    public static CachingSupplier<Map<String, Object>> newQueryScopeVariableSupplier() {
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        return new CachingSupplier<>(() -> Collections.unmodifiableMap(
                queryScope.toMap((name, value) -> NameValidator.isValidQueryParameterName(name))));
    }

    /**
     * @return a CachingSupplier that supplies a snapshot of the current {@link QueryLibrary} package imports
     */
    private static CachingSupplier<Collection<Package>> newPackageImportsSupplier() {
        final QueryLibrary queryLibrary = ExecutionContext.getContext().getQueryLibrary();
        return new CachingSupplier<>(() -> Set.copyOf(queryLibrary.getPackageImports()));
    }

    /**
     * @return a CachingSupplier that supplies a snapshot of the current {@link QueryLibrary} class imports
     */
    private static CachingSupplier<Collection<Class<?>>> newClassImportsSupplier() {
        final QueryLibrary queryLibrary = ExecutionContext.getContext().getQueryLibrary();
        return new CachingSupplier<>(() -> {
            final Collection<Class<?>> classImports = new HashSet<>(queryLibrary.getClassImports());
            // because QueryLibrary is in the context package, without visibility, we need to add these manually
            classImports.add(TrackingWritableRowSet.class);
            classImports.add(WritableColumnSource.class);
            return Collections.unmodifiableCollection(classImports);
        });
    }

    /**
     * @return a CachingSupplier that supplies a snapshot of the current {@link QueryLibrary} static imports
     */
    private static CachingSupplier<Collection<Class<?>>> newStaticImportsSupplier() {
        final QueryLibrary queryLibrary = ExecutionContext.getContext().getQueryLibrary();
        return new CachingSupplier<>(() -> Set.copyOf(queryLibrary.getStaticImports()));
    }

    private final CachingSupplier<Map<String, Object>> queryScopeVariableSupplier = newQueryScopeVariableSupplier();
    private final CachingSupplier<Collection<Package>> packageImportsSupplier = newPackageImportsSupplier();
    private final CachingSupplier<Collection<Class<?>>> classImportsSupplier = newClassImportsSupplier();
    private final CachingSupplier<Collection<Class<?>>> staticImportsSupplier = newStaticImportsSupplier();

    /**
     * @return a lazily cached snapshot of the current query scope variables
     */
    public final Map<String, Object> getQueryScopeVariables() {
        return queryScopeVariableSupplier.get();
    }

    /**
     * @return a lazily cached snapshot of the current {@link QueryLibrary} package imports
     */
    public final Collection<Package> getPackageImports() {
        return packageImportsSupplier.get();
    }

    /**
     * @return a lazily cached snapshot of the current {@link QueryLibrary} class imports
     */
    public final Collection<Class<?>> getClassImports() {
        return classImportsSupplier.get();
    }

    /**
     * @return a lazily cached snapshot of the current {@link QueryLibrary} static imports
     */
    public final Collection<Class<?>> getStaticImports() {
        return staticImportsSupplier.get();
    }

    /**
     * Submit a request for compilation. The QueryCompilerRequestProcessor is not required to immediately compile this
     * request.
     *
     * @param request the request to compile
     */
    public abstract CompletionStageFuture<Class<?>> submit(@NotNull QueryCompilerRequest request);

    /**
     * A QueryCompilerRequestProcessor that immediately compiles requests.
     */
    public static class ImmediateProcessor extends QueryCompilerRequestProcessor {
        private ImmediateProcessor() {
            // force use of static factory method
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
    public static class BatchProcessor extends QueryCompilerRequestProcessor {
        private final List<QueryCompilerRequest> requests = new ArrayList<>();
        private final List<CompletionStageFuture.Resolver<Class<?>>> resolvers = new ArrayList<>();

        private BatchProcessor() {
            // force use of static factory method
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
