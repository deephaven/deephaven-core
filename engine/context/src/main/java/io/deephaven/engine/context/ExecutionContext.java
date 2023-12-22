/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Container for context-specific objects, that can be activated on a thread or passed to certain operations.
 * ExecutionContexts are immutable, and support a builder pattern to create new instances and "with" methods to
 * customize existing ones. Any thread that interacts with the Deephaven engine will need to have an active
 * ExecutionContext.
 */
public class ExecutionContext {

    /**
     * Creates a new builder for an ExecutionContext, capturing the current thread's auth context, update graph, and
     * operation initializer. Typically, this method should be called on a thread that already has an active
     * ExecutionContext, to more easily reuse those.
     *
     * @return a new builder to create an ExecutionContext
     */
    public static Builder newBuilder() {
        ExecutionContext existing = getContext();
        return new Builder()
                .setUpdateGraph(existing.getUpdateGraph())
                .setOperationInitializer(existing.getOperationInitializer());
    }

    public static ExecutionContext makeExecutionContext(boolean isSystemic) {
        return getContext().withSystemic(isSystemic);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ThreadLocal Management
    // -----------------------------------------------------------------------------------------------------------------

    private static volatile ExecutionContext defaultContext = null;

    public static ExecutionContext getDefaultContext() {
        ExecutionContext localContext;
        if ((localContext = defaultContext) == null) {
            synchronized (ExecutionContext.class) {
                if ((localContext = defaultContext) == null) {
                    localContext = defaultContext = new Builder(null)
                            .markSystemic()
                            .build();
                }
            }
        }
        return localContext;
    }

    private static final ThreadLocal<ExecutionContext> currentContext =
            ThreadLocal.withInitial(ExecutionContext::getDefaultContext);

    /**
     * @return an object representing the current execution context or null if the current context is systemic
     */
    public static ExecutionContext getContextToRecord() {
        final ExecutionContext ctxt = currentContext.get();
        if (ctxt.isSystemic) {
            return null;
        }
        return ctxt;
    }

    /**
     * @return an object representing the current execution context
     */
    public static ExecutionContext getContext() {
        return currentContext.get();
    }

    /**
     * Installs the executionContext to be used for the current thread.
     */
    private static void setContext(final ExecutionContext context) {
        currentContext.set(context);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ExecutionContext Implementation
    // -----------------------------------------------------------------------------------------------------------------

    /** True if this execution context is supplied by the system and not the user. */
    private final boolean isSystemic;

    private final AuthContext authContext;
    private final QueryLibrary queryLibrary;
    private final QueryScope queryScope;
    private final QueryCompiler queryCompiler;
    private final UpdateGraph updateGraph;
    private final OperationInitializer operationInitializer;

    private ExecutionContext(
            final boolean isSystemic,
            final AuthContext authContext,
            final QueryLibrary queryLibrary,
            final QueryScope queryScope,
            final QueryCompiler queryCompiler,
            final UpdateGraph updateGraph,
            OperationInitializer operationInitializer) {
        this.isSystemic = isSystemic;
        this.authContext = authContext;
        this.queryLibrary = Objects.requireNonNull(queryLibrary);
        this.queryScope = Objects.requireNonNull(queryScope);
        this.queryCompiler = Objects.requireNonNull(queryCompiler);
        this.updateGraph = Objects.requireNonNull(updateGraph);
        this.operationInitializer = Objects.requireNonNull(operationInitializer);
    }

    /**
     * Returns, or creates, an execution context with the given value for {@code isSystemic} and existing values for the
     * other members.
     *
     * @param isSystemic if the context should be systemic
     * @return the execution context
     */
    public ExecutionContext withSystemic(boolean isSystemic) {
        if (isSystemic == this.isSystemic) {
            return this;
        }
        return new ExecutionContext(isSystemic, authContext, queryLibrary, queryScope, queryCompiler, updateGraph,
                operationInitializer);
    }

    /**
     * Returns, or creates, an execution context with the given value for {@code authContext} and existing values for
     * the other members. This is not intended to be used by user code.
     *
     * @param authContext the auth context to use instead; null values are ignored
     * @return the execution context
     */
    public ExecutionContext withAuthContext(final AuthContext authContext) {
        if (authContext == this.authContext) {
            return this;
        }
        return new ExecutionContext(isSystemic, authContext, queryLibrary, queryScope, queryCompiler, updateGraph,
                operationInitializer);
    }

    /**
     * Returns, or creates, an execution context with the given value for {@code updateGraph} and existing values for
     * the other members. This is not intended to be used by user code.
     *
     * @param updateGraph the update context to use instead
     * @return the execution context
     */
    public ExecutionContext withUpdateGraph(final UpdateGraph updateGraph) {
        if (updateGraph == this.updateGraph) {
            return this;
        }
        return new ExecutionContext(isSystemic, authContext, queryLibrary, queryScope, queryCompiler, updateGraph,
                operationInitializer);
    }

    public ExecutionContext withOperationInitializer(final OperationInitializer operationInitializer) {
        if (operationInitializer == this.operationInitializer) {
            return this;
        }
        return new ExecutionContext(isSystemic, authContext, queryLibrary, queryScope, queryCompiler, updateGraph,
                operationInitializer);
    }

    /**
     * Execute runnable within this execution context.
     */
    public void apply(Runnable runnable) {
        apply(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Executes supplier within this execution context.
     */
    public <T> T apply(Supplier<T> supplier) {
        try (SafeCloseable ignored = open()) {
            // actually evaluate the script
            return supplier.get();
        }
    }

    /**
     * Installs the executionContext and returns a function that will restore the original executionContext.
     *
     * @return a closeable to cleanup the execution context
     */
    public SafeCloseable open() {
        // save the current context
        final ExecutionContext oldExecutionContext = currentContext.get();

        ExecutionContext.setContext(this);

        return () -> {
            // restore the old context
            ExecutionContext.setContext(oldExecutionContext);
        };
    }

    public QueryLibrary getQueryLibrary() {
        return queryLibrary;
    }

    public QueryScope getQueryScope() {
        return queryScope;
    }

    public QueryCompiler getQueryCompiler() {
        return queryCompiler;
    }

    public AuthContext getAuthContext() {
        return authContext;
    }

    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    public OperationInitializer getOperationInitializer() {
        return operationInitializer;
    }

    @SuppressWarnings("unused")
    public static class Builder {
        private boolean isSystemic = false;

        private final AuthContext authContext;

        private QueryLibrary queryLibrary = PoisonedQueryLibrary.INSTANCE;
        private QueryScope queryScope = PoisonedQueryScope.INSTANCE;
        private QueryCompiler queryCompiler = PoisonedQueryCompiler.INSTANCE;
        private UpdateGraph updateGraph = PoisonedUpdateGraph.INSTANCE;
        private OperationInitializer operationInitializer = PoisonedOperationInitializer.INSTANCE;

        private Builder() {
            // propagate the auth context from the current context
            this(getContext().authContext);
        }

        @VisibleForTesting
        Builder(final AuthContext authContext) {
            this.authContext = authContext;
        }

        /**
         * A systemic execution context is one that is supplied by the system and not the user. A systemic context will
         * not be recorded when work is deferred.
         */
        @ScriptApi
        public Builder markSystemic() {
            isSystemic = true;
            return this;
        }

        /**
         * Instantiate an empty query library.
         */
        @ScriptApi
        public Builder newQueryLibrary() {
            queryLibrary = QueryLibrary.makeNewLibrary();
            return this;
        }

        /**
         * Instantiate an empty query library using the provided libraryVersion. This is useful for testing.
         */
        @ScriptApi
        public Builder newQueryLibrary(String libraryVersion) {
            queryLibrary = QueryLibrary.makeNewLibrary(libraryVersion);
            return this;
        }

        /**
         * Use the provided QueryLibrary.
         */
        @ScriptApi
        public Builder setQueryLibrary(final QueryLibrary queryLibrary) {
            this.queryLibrary = queryLibrary;
            return this;
        }

        /**
         * Use the current ExecutionContext's QueryLibrary instance.
         */
        @ScriptApi
        public Builder captureQueryLibrary() {
            queryLibrary = currentContext.get().getQueryLibrary();
            return this;
        }

        /**
         * Use the provided QueryCompiler
         */
        @ScriptApi
        public Builder setQueryCompiler(final QueryCompiler queryCompiler) {
            this.queryCompiler = queryCompiler;
            return this;
        }

        /**
         * Use the current ExecutionContext's QueryCompiler instance.
         */
        @ScriptApi
        public Builder captureQueryCompiler() {
            queryCompiler = currentContext.get().getQueryCompiler();
            return this;
        }

        /**
         * Use a query scope that is immutably empty.
         */
        @ScriptApi
        public Builder emptyQueryScope() {
            this.queryScope = EmptyQueryScope.INSTANCE;
            return this;
        }

        /**
         * Instantiate a new query scope.
         */
        @ScriptApi
        public Builder newQueryScope() {
            this.queryScope = new QueryScope.StandaloneImpl();
            return this;
        }

        /**
         * Use the provided QueryScope.
         */
        @ScriptApi
        public Builder setQueryScope(final QueryScope queryScope) {
            this.queryScope = queryScope;
            return this;
        }

        /**
         * Use the current ExecutionContext's QueryScope instance.
         * <p>
         *
         * @apiNote This captures a reference to the current, likely mutable, query scope state. Future changes to the
         *          query scope may cause deferred operations to fail. Additionally, there is risk of GC leakage should
         *          the query scope be otherwise unreachable.
         */
        @ScriptApi
        public Builder captureQueryScope() {
            this.queryScope = currentContext.get().getQueryScope();
            return this;
        }

        /**
         * Instantiate a new QueryScope and initialize with values from the current ExecutionContext.
         *
         * @param vars the variable names to copy from the current ExecutionContext's QueryScope
         */
        @ScriptApi
        public Builder captureQueryScopeVars(String... vars) {
            if (vars.length == 0) {
                return newQueryScope();
            }

            this.queryScope = new QueryScope.StandaloneImpl();

            final QueryScopeParam<?>[] params = getContext().getQueryScope().getParams(Arrays.asList(vars));
            for (final QueryScopeParam<?> param : params) {
                this.queryScope.putParam(param.getName(), param.getValue());
            }

            return this;
        }

        /**
         * Use the provided UpdateGraph.
         */
        @ScriptApi
        public Builder setUpdateGraph(UpdateGraph updateGraph) {
            this.updateGraph = updateGraph;
            return this;
        }

        /**
         * Use the current ExecutionContext's UpdateGraph instance.
         *
         * @deprecated The update graph is automatically captured, this method should no longer be needed.
         */
        @ScriptApi
        @Deprecated(forRemoval = true, since = "0.31")
        public Builder captureUpdateGraph() {
            this.updateGraph = getContext().getUpdateGraph();
            return this;
        }

        /**
         * Use the specified operation initializer instead of the captured instance.
         */
        @ScriptApi
        public Builder setOperationInitializer(OperationInitializer operationInitializer) {
            this.operationInitializer = operationInitializer;
            return this;
        }

        /**
         * @return the newly instantiated ExecutionContext
         */
        @ScriptApi
        public ExecutionContext build() {
            return new ExecutionContext(isSystemic, authContext, queryLibrary, queryScope, queryCompiler, updateGraph,
                    operationInitializer);
        }
    }
}
