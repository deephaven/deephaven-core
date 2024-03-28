//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.lang.completion;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.context.QueryLibrary;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.util.datastructures.CachingSupplier;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

/**
 * A lookup object for various values that the {@link ChunkerCompleter} might be interested in.
 *
 * This is extracted into its own class, so preloading can start as soon as the console session starts.
 *
 */
public class CompletionLookups {

    private static final WeakHashMap<ScriptSession, CompletionLookups> lookups = new WeakHashMap<>();

    private final CachingSupplier<Collection<Class<?>>> statics;
    private final Map<String, TableDefinition> referencedTables;
    private final CachingSupplier<CustomCompletion> customCompletions;

    public CompletionLookups(Set<CustomCompletion.Factory> customCompletionFactory) {
        final QueryLibrary ql = ExecutionContext.getContext().getQueryLibrary();
        statics = new CachingSupplier<>(ql::getStaticImports);
        referencedTables = new ConcurrentHashMap<>();
        customCompletions = new CachingSupplier<>(() -> new DelegatingCustomCompletion(customCompletionFactory));

        // This can be slow, so lets start it on a background thread right away.
        final ForkJoinPool pool = ForkJoinPool.commonPool();
        pool.execute(statics::get);
    }

    public static CompletionLookups preload(ScriptSession session,
            Set<CustomCompletion.Factory> customCompletionFactory) {
        return lookups.computeIfAbsent(session, s -> new CompletionLookups(customCompletionFactory));
    }

    public Collection<Class<?>> getStatics() {
        return statics.get();
    }

    public Map<String, TableDefinition> getReferencedTables() {
        return referencedTables;
    }

    public CustomCompletion getCustomCompletions() {
        return customCompletions.get();
    }
}
