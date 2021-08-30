package io.deephaven.lang.completion;

import io.deephaven.base.Lazy;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.util.ScriptSession;

import java.util.Collection;
import java.util.Map;
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

    private final Lazy<QueryLibrary> ql;
    private final Lazy<Collection<Class<?>>> statics;
    private final Map<String, TableDefinition> referencedTables;

    public CompletionLookups() {
        ql = new Lazy<>(QueryLibrary::getLibrary);
        statics = new Lazy<>(() -> {
            ql.get();
            return QueryLibrary.getStaticImports();
        });
        referencedTables = new ConcurrentHashMap<>();

        // This can be slow, so lets start it on a background thread right away.
        final ForkJoinPool pool = ForkJoinPool.commonPool();
        pool.execute(statics::get);
    }

    public static CompletionLookups preload(ScriptSession session) {
        return lookups.computeIfAbsent(session, s -> new CompletionLookups());
    }

    public Collection<Class<?>> getStatics() {
        return statics.get();
    }

    public Map<String, TableDefinition> getReferencedTables() {
        return referencedTables;
    }
}
