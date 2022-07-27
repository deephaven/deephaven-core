package io.deephaven.engine.context;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.function.Supplier;

public class ExecutionContextImpl extends ExecutionContext {

    public static class Builder {
        private boolean isSystemic = false;

        private QueryLibrary.Context queryLibrary = QueryLibrary.getDefaultLibrary();
        private QueryScope queryScope = QueryScope.getDefaultScope();
        private CompilerTools.Context compilerContext = CompilerTools.getDefaultContext();

        public Builder markSystemic() {
            isSystemic = true;
            return this;
        }

        public Builder captureQueryLibrary() {
            queryLibrary = QueryLibrary.getLibrary();
            return this;
        }

        public Builder setQueryLibrary(final QueryLibrary.Context queryLibrary) {
            this.queryLibrary = queryLibrary;
            return this;
        }

        public Builder captureCompilerContext() {
            compilerContext = CompilerTools.getContext();
            return this;
        }

        public Builder setCompilerContext(final CompilerTools.Context compilerContext) {
            this.compilerContext = compilerContext;
            return this;
        }

        public Builder newQueryScope() {
            this.queryScope = new QueryScope.StandaloneImpl();
            return this;
        }

        public Builder setQueryScope(final QueryScope queryScope) {
            this.queryScope = queryScope;
            return this;
        }

        public Builder captureMutableQueryScope() {
            this.queryScope = QueryScope.getScope();
            return this;
        }

        public Builder captureQueryScopeVars(String... vars) {
            this.queryScope = new QueryScope.StandaloneImpl();

            final QueryScopeParam<?>[] params = QueryScope.getScope().getParams(Arrays.asList(vars));
            for (final QueryScopeParam<?> param : params) {
                this.queryScope.putParam(param.getName(), param.getValue());
            }

            return this;
        }

        public ExecutionContextImpl build() {
            return new ExecutionContextImpl(isSystemic, queryLibrary, queryScope, compilerContext);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static ExecutionContext makeSystemicExecutionContext() {
        return getCurrentContext(true);
    }

    public static ExecutionContext getCurrentContext() {
        return getCurrentContext(false);
    }

    public static ExecutionContext getCurrentContext(final boolean allowSystemic) {
        final ExecutionContext threadLocal = ExecutionContext.getThreadLocal();

        final QueryLibrary.Context ql = QueryLibrary.getLibrary();
        final QueryScope qs = QueryScope.getScope();
        final CompilerTools.Context cc = CompilerTools.getContext();

        // only return the installed execution context if the thread locals still match
        if ((threadLocal instanceof ExecutionContextImpl)) {
            ExecutionContextImpl tl = (ExecutionContextImpl) threadLocal;
            if (tl.getQueryLibrary() == ql && tl.getQueryScope() == qs && tl.getCompilerContext() == cc) {
                return tl;
            }
        }

        if (allowSystemic) {
            return newBuilder()
                    .setQueryLibrary(ql)
                    .setCompilerContext(cc)
                    .setQueryScope(qs)
                    .markSystemic()
                    .build();
        }
        return null;
    }

    @VisibleForTesting
    public static SafeCloseable createForUnitTests() {
        CompilerTools.setContextForUnitTests();
        QueryLibrary.setLibrary(QueryLibrary.makeNewLibrary());
        QueryScope.setScope(new QueryScope.StandaloneImpl());

        return () -> {
            CompilerTools.resetContext();
            QueryLibrary.resetLibrary();
            QueryScope.resetScope();
        };
    }

    /** True if this execution context is supplied by the system and not the user. */
    private final boolean isSystemic;

    private final QueryLibrary.Context queryLibrary;
    private final QueryScope queryScope;
    private final CompilerTools.Context compilerContext;

    public ExecutionContextImpl(
            final boolean isSystemic,
            final QueryLibrary.Context queryLibrary,
            final QueryScope queryScope,
            final CompilerTools.Context compilerContext) {
        this.isSystemic = isSystemic;
        this.queryLibrary = queryLibrary;
        this.queryScope = queryScope;
        this.compilerContext = compilerContext;
    }

    @Override
    public void apply(Runnable runnable) {
        apply(() -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public <T> T apply(Supplier<T> supplier) {
        try (SafeCloseable ignored = open()) {
            // actually evaluate the script
            return supplier.get();
        }
    }

    @Override
    public SafeCloseable open() {
        // store pointers to exist query scope static variables
        final QueryLibrary.Context prevQueryLibrary = QueryLibrary.getLibrary();
        final CompilerTools.Context prevCompilerContext = CompilerTools.getContext();
        final QueryScope prevQueryScope = QueryScope.getScope();
        final ExecutionContext oldExecutionContext = ExecutionContext.getThreadLocal();

        // point query scope static state to our session's state
        QueryScope.setScope(queryScope);
        QueryLibrary.setLibrary(queryLibrary);
        CompilerTools.setContext(compilerContext);
        if (!isSystemic) {
            ExecutionContext.setContext(this);
        }

        return () -> {
            // restore pointers to query scope static variables
            QueryScope.setScope(prevQueryScope);
            QueryLibrary.setLibrary(prevQueryLibrary);
            CompilerTools.setContext(prevCompilerContext);
            if (!isSystemic) {
                ExecutionContext.setContext(oldExecutionContext);
            }
        };
    }

    public QueryLibrary.Context getQueryLibrary() {
        return queryLibrary;
    }

    public QueryScope getQueryScope() {
        return queryScope;
    }

    public CompilerTools.Context getCompilerContext() {
        return compilerContext;
    }
}
