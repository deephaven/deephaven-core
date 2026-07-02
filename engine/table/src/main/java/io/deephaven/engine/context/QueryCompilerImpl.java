//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import com.google.common.hash.Hashing;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.util.SynchronizedJavaFileManager;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link QueryCompiler} implementation that compiles Java source to bytecode in memory and defines the resulting
 * classes via {@link ClassLoader#defineClass}, avoiding filesystem writes for compilation output.
 *
 * <p>
 * The compiler resolves dependencies from {@code java.class.path} and an optional additional classpath directory
 * (typically where Groovy writes its bytecode). Compiled classes are loaded in per-batch child classloaders of the
 * provided parent classloader, enabling GC of both classes and classloaders when neither the compiled Classes nor this
 * compiler instance are reachable.
 */
public class QueryCompilerImpl implements QueryCompiler, LogOutputAppendable {

    private static final Logger log = LoggerFactory.getLogger(QueryCompilerImpl.class);
    /**
     * We pick a number just shy of 65536, leaving a little elbow room for good luck.
     */
    private static final int DEFAULT_MAX_STRING_LITERAL_LENGTH = 65500;

    private static final String JAVA_CLASS_VERSION = System.getProperty("java.class.version").replace('.', '_');

    private static final String IDENTIFYING_FIELD_NAME = "_CLASS_BODY_";

    private static boolean logEnabled = Configuration.getInstance().getBoolean("QueryCompiler.logEnabledDefault");

    private static final String TRACE_PREFIX_PROPERTY = "QueryCompiler.tracePrefixes";
    private static final String TRACE_EXCLUDE_PREFIX_PROPERTY = "QueryCompiler.excludeTracePrefixes";
    private static final Set<String> TRACE_INCLUDE_PREFIXES = computeTracePackages(TRACE_PREFIX_PROPERTY);
    private static final Set<String> TRACE_EXCLUDE_PREFIXES = computeTracePackages(TRACE_EXCLUDE_PREFIX_PROPERTY);

    private static Set<String> computeTracePackages(final String property) {
        if (!Configuration.getInstance().hasProperty(property)) {
            return Collections.emptySet();
        }
        final String propertyValue = Configuration.getInstance().getProperty(property);
        return Arrays.stream(propertyValue.split(",")).map(String::trim).collect(Collectors.toSet());
    }

    /**
     * Should this class (or package) be traced? Even with only trace logging, we may not want to flood the log with
     * "uninteresting" classes, so we provide an inclusion and an exclusion list.
     *
     * <p>
     * Excludes take precedence over includes.
     * </p>
     *
     * @param className the class/package name to check against our trace prefixes
     * @return if this class/package should be traced
     */
    private static boolean shouldTrace(String className) {
        if (!log.isTraceEnabled()) {
            return false;
        }
        if (TRACE_EXCLUDE_PREFIXES.stream().anyMatch(className::startsWith)) {
            return false;
        }
        return TRACE_INCLUDE_PREFIXES.stream().anyMatch(className::startsWith);
    }


    private static JavaCompiler compiler;
    private static final AtomicReference<JavaFileManager> fileManagerCache = new AtomicReference<>();

    private static void ensureJavaCompiler() {
        synchronized (QueryCompilerImpl.class) {
            if (compiler == null) {
                compiler = ToolProvider.getSystemJavaCompiler();
                if (compiler == null) {
                    throw new UncheckedDeephavenException(
                            "No Java compiler provided - are you using a JRE instead of a JDK?");
                }
            }
        }
    }

    private static JavaFileManager acquireFileManager() {
        JavaFileManager fileManager = fileManagerCache.getAndSet(null);
        if (fileManager == null) {
            fileManager = new SynchronizedJavaFileManager(compiler.getStandardFileManager(null, null, null));
        }
        return fileManager;
    }

    private static void releaseFileManager(@NotNull final JavaFileManager fileManager) {
        // Reusing the file manager saves a lot of the time in the compilation process. However, we need to be careful
        // to avoid keeping too many file handles open so we'll limit ourselves to just one outstanding file manager.
        if (!fileManagerCache.compareAndSet(null, fileManager)) {
            try {
                fileManager.close();
            } catch (final IOException err) {
                throw new UncheckedIOException("Could not close JavaFileManager", err);
            }
        }
    }

    public static final String FORMULA_CLASS_PREFIX = "io.deephaven.temp";
    public static final String DYNAMIC_CLASS_PREFIX = "io.deephaven.dynamic";

    /**
     * Creates a new QueryCompilerImpl. Uses the current thread's context classloader as the parent for per-batch
     * classloaders (as required by {@link QueryCompiler}'s contract).
     *
     * @param additionalClassPathDir optional directory to add to the compiler's classpath (e.g., groovy bytecode dir)
     */
    public static QueryCompiler create(@Nullable final File additionalClassPathDir) {
        return create(additionalClassPathDir, null);
    }

    /**
     * Creates a new QueryCompilerImpl. Uses the current thread's context classloader as the parent for per-batch
     * classloaders (as required by {@link QueryCompiler}'s contract).
     *
     * @param additionalClassPathDir optional directory to add to the compiler's classpath (e.g., groovy bytecode dir)
     */
    public static QueryCompiler create(@Nullable final File additionalClassPathDir, @Nullable ClassLoader classLoader) {
        return new QueryCompilerImpl(additionalClassPathDir, classLoader);
    }

    /**
     * Creates a new compiler that has no extra directory to read from, suitable for unit tests or cases where the
     * existing classpath is sufficient.
     */
    public static QueryCompiler create() {
        return create(null, null);
    }

    /**
     * Cache from SHA-256 hash of class body to entry. While compilation is in-flight, the entry holds a future for
     * coordination. Once complete, the entry transitions to a weak reference so the class (and its classloader and
     * bytecode) can be GC'd when no longer in use. All transitions are atomic via per-entry synchronization. Stale
     * entries are cleaned up via a {@link ReferenceQueue} drained at the start of each {@link #compile} call.
     */
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    /** Queue that receives cleared weak references, allowing us to remove stale cache entries. */
    private final ReferenceQueue<Class<?>> staleQueue = new ReferenceQueue<>();

    /** Drains the stale queue and removes any cache entries whose weak references have been cleared by GC. */
    private void evictStaleEntries() {
        KeyedWeakReference ref;
        while ((ref = (KeyedWeakReference) staleQueue.poll()) != null) {
            cache.remove(ref.key);
        }
    }

    /**
     * A WeakReference that remembers its cache key, so we can remove the entry when the referent is GC'd.
     */
    private static final class KeyedWeakReference extends WeakReference<Class<?>> {
        final String key;

        KeyedWeakReference(@NotNull Class<?> referent, @NotNull String key,
                @NotNull ReferenceQueue<Class<?>> queue) {
            super(referent, queue);
            this.key = key;
        }
    }

    /**
     * A cache entry that transitions from in-flight (future-based coordination) to weakly-cached (reclaimable).
     * Thread-safe: all reads/writes are synchronized on the entry itself. The entry self-transitions to weak-ref state
     * when its future completes successfully, via a callback registered at construction time.
     *
     * <p>
     * Safety invariant: {@code CompletionStageFutureImpl} (extends {@code CompletableFuture}) retains its result
     * strongly in an internal field even after listeners fire. This means any thread holding a reference to the future
     * can still retrieve the class via {@code get()}, even after this entry has transitioned to weak-ref state. The
     * weak reference only governs the <em>cache's</em> retention — not in-flight callers'.
     * </p>
     */
    private static final class CacheEntry {
        private CompletionStageFuture<Class<?>> future;
        private WeakReference<Class<?>> classRef;

        CacheEntry(@NotNull CompletionStageFuture<Class<?>> future,
                @NotNull String key,
                @NotNull ReferenceQueue<Class<?>> queue) {
            this.future = future;
            // Self-transition: when compilation completes, swap to keyed weak ref
            future.whenComplete((clazz, err) -> {
                if (clazz != null) {
                    complete(clazz, key, queue);
                }
                // On failure, the entry stays in-flight with a failed future.
                // The caller will remove it from the cache.
            });
        }

        /** Transition to weak-ref state: releases the strong reference held by the completed future. */
        private synchronized void complete(
                @NotNull Class<?> clazz, @NotNull String key, @NotNull ReferenceQueue<Class<?>> queue) {
            this.classRef = new KeyedWeakReference(clazz, key, queue);
            this.future = null;
        }

        /**
         * Atomically resolve this entry's state. Returns one of:
         * <ul>
         * <li>A {@code Class<?>} — cache hit, class still alive</li>
         * <li>A {@code CompletionStageFuture<Class<?>>} — compilation in flight, wait on it</li>
         * <li>{@code null} — entry is stale (class was GC'd), caller should retry</li>
         * </ul>
         */
        synchronized Optional<Future<Class<?>>> resolve() {
            if (classRef != null) {
                final Class<?> c = classRef.get();
                return Optional.ofNullable(c).map(CompletableFuture::completedFuture);
            }
            return Optional.of(future);
        }

        /** True if completed but the class has been GC'd (stale). Used inside compute() lambda. */
        synchronized boolean isStale() {
            return future == null && (classRef == null || classRef.get() == null);
        }
    }

    /** The context classloader provided at creation time, or null if none set */
    @Nullable
    private final ClassLoader parentClassLoader;

    /** Optional additional classpath directory (e.g., where Groovy writes bytecode). */
    @Nullable
    private final File additionalClassPathDir;

    private QueryCompilerImpl(
            @Nullable final File additionalClassPathDir,
            @Nullable final ClassLoader parentClassLoader) {
        ensureJavaCompiler();
        this.additionalClassPathDir = additionalClassPathDir;
        this.parentClassLoader = parentClassLoader;

        if (log.isTraceEnabled()) {
            log.trace().append("QueryCompiler Class Path: ").append(getClassPath()).append(File.pathSeparator)
                    .append(getJavaClassPath()).endl();
        }
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("QueryCompilerImpl{additionalClassPathDir=")
                .append(additionalClassPathDir == null ? "null" : additionalClassPathDir.getAbsolutePath())
                .append("}");
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    /**
     * Enables or disables compilation logging.
     *
     * @param logEnabled Whether logging should be enabled
     * @return The value of {@code logEnabled} before calling this method.
     */
    public static boolean setLogEnabled(boolean logEnabled) {
        boolean original = QueryCompilerImpl.logEnabled;
        QueryCompilerImpl.logEnabled = logEnabled;
        return original;
    }

    @Override
    public void compile(
            @NotNull final QueryCompilerRequest[] requests,
            @NotNull final CompletionStageFuture.Resolver<Class<?>>[] resolvers) {
        if (requests.length == 0) {
            return;
        }
        if (requests.length != resolvers.length) {
            throw new IllegalArgumentException("Requests and resolvers must be the same length");
        }

        // Opportunistically clean up stale cache entries whose classes have been GC'd
        evictStaleEntries();

        final boolean shouldTrace =
                log.isTraceEnabled() && Arrays.stream(requests).map(QueryCompilerRequest::packageNameRoot)
                        .anyMatch(QueryCompilerImpl::shouldTrace);
        if (shouldTrace) {
            log.trace().append("Compilation request for ").append((logOutput, queryCompilerRequests) -> {
                logOutput.append("[");
                for (int ii = 0; ii < queryCompilerRequests.length; ++ii) {
                    if (ii > 0) {
                        logOutput.append(", ");
                    }
                    requests[ii].appendSummary(logOutput);
                }
                logOutput.append("]");
            }, requests).endl();
        }

        // For each request: cache hit, in-flight wait, or needs compilation
        final List<QueryCompilerRequest> newRequests = new ArrayList<>();
        final List<CompletionStageFuture.Resolver<Class<?>>> newResolvers = new ArrayList<>();
        final List<String> newHashes = new ArrayList<>();
        final List<Future<Class<?>>> allFutures = new ArrayList<>(requests.length);

        for (int ii = 0; ii < requests.length; ++ii) {
            // Compute content hash for deduplication
            QueryCompilerRequest req = requests[ii];
            String hash = Hashing.sha256().hashString(req.classBody(), StandardCharsets.UTF_8).toString();

            final CompletionStageFuture.Resolver<Class<?>> resolver = resolvers[ii];

            // Resolve cache state, retrying if we hit a stale entry race
            Optional<Future<Class<?>>> resolved;
            while (true) {
                final CacheEntry entry = cache.compute(hash, (key, existing) -> {
                    if (existing != null && !existing.isStale()) {
                        return existing;
                    }

                    // No/stale existing entry, we will do the compilation ourselves
                    newResolvers.add(resolver);
                    newRequests.add(req);
                    newHashes.add(hash);
                    return new CacheEntry(resolver.getFuture(), key, staleQueue);
                });

                resolved = entry.resolve();
                if (resolved.isPresent()) {
                    // Got a future, either to our work, or someone else's
                    allFutures.add(resolved.get());
                    break;
                }
                // Stale race: entry transitioned between compute() and resolve(). Remove and retry.
                cache.remove(hash, entry);
            }
        }

        // Compile new requests
        if (!newResolvers.isEmpty()) {
            try {
                compileHelper(newRequests, newResolvers, newHashes);
            } catch (RuntimeException e) {
                for (int ii = 0; ii < newHashes.size(); ++ii) {
                    if (newResolvers.get(ii).completeExceptionally(e)) {
                        cache.remove(newHashes.get(ii));
                    }
                }
                throw e;
            }

            // Remove cache entries for failed or incomplete compilations
            // (successful ones self-transitioned to weak refs via whenComplete)
            for (int ii = 0; ii < newHashes.size(); ++ii) {
                try {
                    newResolvers.get(ii).getFuture().get(); // throws if completed exceptionally
                } catch (Exception ex) {
                    cache.remove(newHashes.get(ii));
                }
            }
        }

        // Wait on in-flight futures from other compilations
        for (int ii = 0; ii < requests.length; ++ii) {
            final Future<Class<?>> future = allFutures.get(ii);
            try {
                resolvers[ii].complete(future.get());
            } catch (ExecutionException err) {
                resolvers[ii].completeExceptionally(err.getCause());
            } catch (InterruptedException err) {
                // This can only occur if we are interrupted while waiting for the future to complete from another
                // compilation request.
                Assert.notEquals(resolvers[ii], "resolvers[ii]", allFutures.get(ii), "allFutures.get(ii)");
                resolvers[ii].completeExceptionally(err);
            } catch (Throwable err) {
                resolvers[ii].completeExceptionally(err);
            }
        }
    }

    private void compileHelper(
            @NotNull final List<QueryCompilerRequest> requests,
            @NotNull final List<CompletionStageFuture.Resolver<Class<?>>> resolvers,
            @NotNull final List<String> hashes) {
        // Assign unique FQ class names for each request using pre-computed hashes
        final String[] fqClassNames = new String[requests.size()];
        final String[] packageNames = new String[requests.size()];

        for (int ii = 0; ii < requests.size(); ++ii) {
            final QueryCompilerRequest request = requests.get(ii);
            final String packageNameSuffix = "c_" + hashes.get(ii) + "v" + JAVA_CLASS_VERSION;
            packageNames[ii] = request.getPackageName(packageNameSuffix);
            fqClassNames[ii] = packageNames[ii] + "." + request.className();
        }

        // Build compilation attempts
        final List<CompilationRequestAttempt> attempts = new ArrayList<>();
        for (int ii = 0; ii < requests.size(); ++ii) {
            attempts.add(new CompilationRequestAttempt(
                    requests.get(ii), packageNames[ii], fqClassNames[ii], resolvers.get(ii)));
        }

        // Compile and define
        compileAndDefine(attempts);

        // Validate _CLASS_BODY_ field on successfully defined classes
        for (int ii = 0; ii < requests.size(); ++ii) {
            final CompletionStageFuture.Resolver<Class<?>> resolver = resolvers.get(ii);
            if (resolver.getFuture().isDone()) {
                continue; // already completed (success or failure)
            }
            // This shouldn't happen - compileAndDefine should have resolved everything
            resolver.completeExceptionally(new IllegalStateException(
                    "Class was not resolved after compilation: " + fqClassNames[ii]));
        }
    }

    private void compileAndDefine(@NotNull final List<CompilationRequestAttempt> requests) {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final int parallelismFactor = executionContext.getOperationInitializer().parallelismFactor();

        final int requestsPerTask = Math.max(32, (requests.size() + parallelismFactor - 1) / parallelismFactor);

        final int numTasks;
        final JobScheduler jobScheduler;

        final boolean canParallelize = executionContext.getOperationInitializer().canParallelize();
        if (!canParallelize || parallelismFactor == 1 || requestsPerTask >= requests.size()) {
            numTasks = 1;
            jobScheduler = new ImmediateJobScheduler();
        } else {
            numTasks = (requests.size() + requestsPerTask - 1) / requestsPerTask;
            jobScheduler = new OperationInitializerJobScheduler();
        }

        log.trace().append("maybeCreateClasses: ").append(requests.size()).append(" requests, ")
                .append(numTasks).append(" tasks, ").append(requestsPerTask).endl();

        final JavaFileManager fileManager = acquireFileManager();
        final AtomicReference<RuntimeException> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Runnable cleanup = () -> {
            try {
                releaseFileManager(fileManager);
            } catch (Exception e) {
                // ignore errors here
            } finally {
                latch.countDown();
            }
        };

        final Consumer<Exception> onError = err -> {
            if (err instanceof RuntimeException) {
                exception.set((RuntimeException) err);
            } else {
                exception.set(new UncheckedDeephavenException("Error during compilation", err));
            }
            cleanup.run();
        };

        jobScheduler.iterateParallel(executionContext, null, JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, numTasks, (context, jobId, nestedErrorConsumer) -> {
                    final int startInclusive = jobId * requestsPerTask;
                    final int endExclusive = Math.min(requests.size(), (jobId + 1) * requestsPerTask);
                    doCompileAndDefine(fileManager, requests, startInclusive, endExclusive);
                },
                () -> {
                },
                cleanup,
                onError);

        try {
            latch.await();
            final BasePerformanceEntry perfEntry = jobScheduler.getAccumulatedPerformance();
            if (perfEntry != null) {
                QueryPerformanceRecorder.getInstance().getEnclosingNugget().accumulate(perfEntry);
            }
            final RuntimeException err = exception.get();
            if (err != null) {
                throw err;
            }
        } catch (final InterruptedException e) {
            throw new CancellationException("interrupted while compiling");
        }
    }

    private void doCompileAndDefine(
            @NotNull final JavaFileManager fileManager,
            @NotNull final List<CompilationRequestAttempt> requests,
            final int startInclusive,
            final int endExclusive) {
        final List<CompilationRequestAttempt> toRetry = new ArrayList<>();
        final boolean wantRetry = doCompileAndDefineSingleRound(
                fileManager, requests, startInclusive, endExclusive, toRetry);
        if (!wantRetry) {
            return;
        }
        // Retry non-failing requests from the first pass
        final List<CompilationRequestAttempt> ignored = new ArrayList<>();
        if (doCompileAndDefineSingleRound(fileManager, toRetry, 0, toRetry.size(), ignored)) {
            throw new IllegalStateException("Unexpected failure during second pass of compilation");
        }
    }

    private boolean doCompileAndDefineSingleRound(
            @NotNull final JavaFileManager fileManager,
            @NotNull final List<CompilationRequestAttempt> requests,
            final int startInclusive,
            final int endExclusive,
            @NotNull final List<CompilationRequestAttempt> toRetry) {

        // Create an in-memory file manager that captures compiled output
        final InMemoryOutputFileManager outputFm = new InMemoryOutputFileManager(fileManager);
        final StringWriter compilerOutput = new StringWriter();

        final String classPathAsString = getClassPath();
        final List<String> compilerOptions = Arrays.asList(
                "-cp", classPathAsString,
                // this option allows the compiler to attempt to process all source files even if some of them fail
                "--should-stop=ifError=GENERATE");

        final MutableInt numFailures = new MutableInt(0);
        final List<RuntimeException> globalFailures = new ArrayList<>();
        compiler.getTask(compilerOutput,
                outputFm,
                diagnostic -> {
                    if (diagnostic.getKind() != Diagnostic.Kind.ERROR) {
                        return;
                    }

                    final JavaSourceFromString source = (JavaSourceFromString) diagnostic.getSource();

                    if (source == null) {
                        // If we have no source, then mark every request as a failure.
                        final UncheckedDeephavenException err = new UncheckedDeephavenException(
                                "Error Invoking Compiler, no source present in diagnostic:\n"
                                        + diagnostic.getMessage(Locale.getDefault()));
                        globalFailures.add(err);
                        return;
                    }

                    final UncheckedDeephavenException err = new UncheckedDeephavenException("Error Compiling "
                            + source.description + "\n" + diagnostic.getMessage(Locale.getDefault()));
                    if (source.resolver.completeExceptionally(err)) {
                        // only count the first failure for each source
                        numFailures.increment();
                    }
                },
                compilerOptions,
                null,
                requests.subList(startInclusive, endExclusive).stream()
                        .map(CompilationRequestAttempt::makeSource)
                        .collect(Collectors.toList()))
                .call();

        final String compilerOutputText = compilerOutput.toString();
        if (!compilerOutputText.isEmpty()) {
            log.trace().append("Compiler output:\n").append(compilerOutputText).endl();
        }

        if (!globalFailures.isEmpty()) {
            final RuntimeException e0 = globalFailures.get(0);
            for (int ii = 1; ii < globalFailures.size(); ++ii) {
                e0.addSuppressed(globalFailures.get(ii));
            }
            throw e0;
        }

        final boolean wantRetry = numFailures.get() > 0 && numFailures.get() != endExclusive - startInclusive;

        // Define compiled classes into a per-batch classloader
        final Map<String, byte[]> compiledClasses = outputFm.getCompiledClasses();
        log.trace().append("compilation produced ").append(compiledClasses.size())
                .append(" classes, numFailures=").append(numFailures.get())
                .append(", range=[").append(startInclusive).append(",").append(endExclusive).append(")")
                .endl();
        if (!compiledClasses.isEmpty()) {
            // Use the current thread's context classloader as parent, unless we were explicitly provided a different
            // one
            final ClassLoader batchParent =
                    parentClassLoader != null ? parentClassLoader : Thread.currentThread().getContextClassLoader();;
            final BatchClassLoader batchCl = new BatchClassLoader(batchParent, compiledClasses);

            for (final CompilationRequestAttempt request : requests.subList(startInclusive, endExclusive)) {
                if (request.resolver.getFuture().isDone()) {
                    // already failed
                    continue;
                }

                if (!compiledClasses.containsKey(request.fqClassName)) {
                    if (wantRetry) {
                        toRetry.add(request);
                    }
                    continue;
                }

                // Load the outer class for this request
                final Class<?> clazz;
                try {
                    clazz = batchCl.loadClass(request.fqClassName);
                } catch (ClassNotFoundException e) {
                    request.resolver.completeExceptionally(new UncheckedDeephavenException(
                            "Failed to load compiled class: " + request.fqClassName, e));
                    continue;
                }

                // Validate the identifying field
                final String identifyingFieldValue = loadIdentifyingField(clazz);
                if (!request.request.classBody().equals(identifyingFieldValue)) {
                    request.resolver.completeExceptionally(new IllegalStateException(
                            "Compiled class body validation failed for " + request.fqClassName));
                    continue;
                }

                // Notify caller with codeLog if requested
                request.request.codeLog().ifPresent(
                        sb -> sb.append(makeFinalCode(
                                request.request.className(), request.request.classBody(), request.packageName)));

                // Complete the future
                log.trace().append("Resolving ").append(request.fqClassName).endl();
                request.resolver.complete(clazz);
            }
        } else {
            // No output at all - if there are non-failed requests, they need retry
            for (final CompilationRequestAttempt request : requests.subList(startInclusive, endExclusive)) {
                if (!request.resolver.getFuture().isDone() && wantRetry) {
                    toRetry.add(request);
                }
            }
        }

        return wantRetry && !toRetry.isEmpty();
    }

    // --- Classpath construction ---

    private String getClassPath() {
        final StringBuilder sb = new StringBuilder(getJavaClassPath());
        if (additionalClassPathDir != null) {
            sb.append(File.pathSeparator).append(additionalClassPathDir.getAbsolutePath());
        }
        return sb.toString();
    }

    // --- BatchClassLoader ---

    /**
     * A classloader that defines classes from a map of name→bytes. All classes from a single compilation batch are
     * loaded together. The parent classloader handles all other class resolution.
     */
    private static class BatchClassLoader extends ClassLoader {
        private final Map<String, byte[]> classBytes;

        BatchClassLoader(@NotNull ClassLoader parent, @NotNull Map<String, byte[]> classBytes) {
            super(parent);
            this.classBytes = classBytes;
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            final byte[] bytes = classBytes.get(name);
            if (bytes != null) {
                return defineClass(name, bytes, 0, bytes.length);
            }
            throw new ClassNotFoundException(name);
        }
    }

    // --- InMemoryOutputFileManager ---

    /**
     * A forwarding JavaFileManager that intercepts class output, storing compiled bytecode in memory rather than
     * writing to the filesystem. All other operations are delegated.
     */
    private static class InMemoryOutputFileManager extends ForwardingJavaFileManager<JavaFileManager> {
        private final ConcurrentHashMap<String, InMemoryClassFileObject> outputClasses = new ConcurrentHashMap<>();

        InMemoryOutputFileManager(JavaFileManager delegate) {
            super(delegate);
        }

        Map<String, byte[]> getCompiledClasses() {
            final Map<String, byte[]> result = new HashMap<>();
            for (Map.Entry<String, InMemoryClassFileObject> entry : outputClasses.entrySet()) {
                result.put(entry.getKey(), entry.getValue().getBytes());
            }
            return result;
        }

        @Override
        public JavaFileObject getJavaFileForOutput(
                Location location, String className, JavaFileObject.Kind kind, FileObject sibling) {
            if (location != StandardLocation.CLASS_OUTPUT || kind != JavaFileObject.Kind.CLASS) {
                throw new IllegalArgumentException("In-memory output file manager only supports writing bytecode");
            }
            final InMemoryClassFileObject fileObject = new InMemoryClassFileObject(className);
            InMemoryClassFileObject existing = outputClasses.put(className, fileObject);
            if (existing != null) {
                throw new IllegalArgumentException("Attempted to write duplicate class name " + className);
            }
            return fileObject;
        }

        @Override
        public boolean hasLocation(Location location) {
            if (location == StandardLocation.CLASS_OUTPUT) {
                return true;
            }
            return super.hasLocation(location);
        }
    }

    // --- InMemoryClassFileObject ---

    private static class InMemoryClassFileObject extends SimpleJavaFileObject {
        private final String className;
        private ByteArrayOutputStream outputStream;

        InMemoryClassFileObject(String className) {
            super(URI.create("mem:///" + className.replace('.', '/') + Kind.CLASS.extension), Kind.CLASS);
            this.className = className;
        }

        @Override
        public OutputStream openOutputStream() {
            outputStream = new ByteArrayOutputStream();
            return outputStream;
        }

        byte[] getBytes() {
            if (outputStream == null) {
                throw new IllegalStateException("No bytes available for " + className);
            }
            return outputStream.toByteArray();
        }
    }

    // --- Source representation ---

    private static class JavaSourceFromString extends SimpleJavaFileObject {
        final String description;
        final String code;
        final CompletionStageFuture.Resolver<Class<?>> resolver;

        JavaSourceFromString(
                final String description,
                final String name,
                final String code,
                final CompletionStageFuture.Resolver<Class<?>> resolver) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.description = description;
            this.code = code;
            this.resolver = resolver;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private static class CompilationRequestAttempt {
        final String description;
        final String fqClassName;
        final String finalCode;
        final String packageName;
        final QueryCompilerRequest request;
        final CompletionStageFuture.Resolver<Class<?>> resolver;

        private CompilationRequestAttempt(
                @NotNull final QueryCompilerRequest request,
                @NotNull final String packageName,
                @NotNull final String fqClassName,
                @NotNull final CompletionStageFuture.Resolver<Class<?>> resolver) {
            this.description = request.description();
            this.fqClassName = fqClassName;
            this.resolver = resolver;
            this.packageName = packageName;
            this.request = request;

            finalCode = makeFinalCode(request.className(), request.classBody(), packageName);

            if (logEnabled) {
                log.info().append("Generating code ").append(finalCode).endl();
            }
        }

        JavaSourceFromString makeSource() {
            return new JavaSourceFromString(description, fqClassName, finalCode, resolver);
        }
    }

    // --- Utilities ---

    private static String loadIdentifyingField(Class<?> c) {
        try {
            final Field field = c.getDeclaredField(IDENTIFYING_FIELD_NAME);
            return (String) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Malformed class in cache", e);
        }
    }

    private static String makeFinalCode(String className, String classBody, String packageName) {
        if (classBody.contains("$CLASSNAME$")) {
            throw new IllegalArgumentException("QueryCompiler's support of the $CLASSNAME$ variable has been removed as"
                    + " the final class name affects the compiled byte code and therefore cannot be dynamically "
                    + "replaced.");
        }

        final String joinedEscapedBody = createEscapedJoinedString(classBody);
        classBody = classBody.substring(0, classBody.lastIndexOf("}"));
        classBody += "    public static String " + IDENTIFYING_FIELD_NAME + " = " + joinedEscapedBody + ";\n}";
        return "package " + packageName + ";\n" + classBody;
    }

    /**
     * Transform a string into the corresponding Java source code that compiles into that string.
     */
    public static String createEscapedJoinedString(final String originalString) {
        return createEscapedJoinedString(originalString, DEFAULT_MAX_STRING_LITERAL_LENGTH);
    }

    public static String createEscapedJoinedString(final String originalString, int maxStringLength) {
        final String[] splits = splitByModifiedUtf8Encoding(originalString, maxStringLength);

        for (int ii = 0; ii < splits.length; ++ii) {
            final String escaped = StringEscapeUtils.escapeJava(splits[ii]);
            splits[ii] = "\"" + escaped + "\"";
        }
        assert splits.length > 0;
        if (splits.length == 1) {
            return splits[0];
        }
        final String formattedInnards = String.join(",\n", splits);
        return "String.join(\"\", " + formattedInnards + ")";
    }

    private static String[] splitByModifiedUtf8Encoding(final String originalString, int maxBytes) {
        final List<String> splits = new ArrayList<>();
        int previousEnd = 0;
        int currentByteCount = 0;
        for (int ii = 0; ii < originalString.length(); ++ii) {
            final int bytesConsumed = calcBytesConsumed(originalString.charAt(ii));
            if (currentByteCount + bytesConsumed > maxBytes) {
                splits.add(originalString.substring(previousEnd, ii));
                previousEnd = ii;
                currentByteCount = 0;
            }
            currentByteCount += bytesConsumed;
        }
        splits.add(originalString.substring(previousEnd));
        return splits.toArray(String[]::new);
    }

    private static int calcBytesConsumed(final char ch) {
        if (ch == 0) {
            return 2;
        }
        if (ch <= 0x7f) {
            return 1;
        }
        if (ch <= 0x7ff) {
            return 2;
        }
        return 3;
    }

    /**
     * @return the java class path from our existing Java class path, and IntelliJ/TeamCity environment variables
     */
    private static String getJavaClassPath() {
        String javaClasspath;
        {
            final StringBuilder javaClasspathBuilder = new StringBuilder(System.getProperty("java.class.path"));

            final String teamCityWorkDir = System.getProperty("teamcity.build.workingDir");
            if (teamCityWorkDir != null) {
                // We are running in TeamCity, get the classpath differently
                final File[] classDirs = new File(teamCityWorkDir + "/_out_/classes").listFiles();
                if (classDirs != null) {
                    for (File f : classDirs) {
                        javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                    }
                }

                final File[] testDirs = new File(teamCityWorkDir + "/_out_/test-classes").listFiles();
                if (testDirs != null) {
                    for (File f : testDirs) {
                        javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                    }
                }

                final File[] jars = FileUtils.findAllFiles(new File(teamCityWorkDir + "/lib"));
                for (File f : jars) {
                    if (f.getName().endsWith(".jar")) {
                        javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                    }
                }
            }
            javaClasspath = javaClasspathBuilder.toString();
        }

        // IntelliJ will bundle a very large class path into an empty jar with a Manifest that will define the full
        // class path. Look for this being used during compile time, so the full class path can be sent into the compile
        // call.
        final String intellijClassPathJarRegex = ".*classpath[0-9]*\\.jar.*";
        if (javaClasspath.matches(intellijClassPathJarRegex)) {
            try {
                final Enumeration<URL> resources =
                        QueryCompilerImpl.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
                final Attributes.Name createdByAttribute = new Attributes.Name("Created-By");
                final Attributes.Name classPathAttribute = new Attributes.Name("Class-Path");
                while (resources.hasMoreElements()) {
                    // Check all manifests -- looking for the Intellij created one
                    final Manifest manifest = new Manifest(resources.nextElement().openStream());
                    final Attributes attributes = manifest.getMainAttributes();
                    final Object createdBy = attributes.get(createdByAttribute);
                    if ("IntelliJ IDEA".equals(createdBy)) {
                        final String extendedClassPath = (String) attributes.get(classPathAttribute);
                        if (extendedClassPath != null) {
                            // Parses the files in the manifest description an changes their format to drop the "file:/"
                            // and use the default path separator
                            final String filePaths = Stream.of(extendedClassPath.split("file:/"))
                                    .map(String::trim)
                                    .filter(fileName -> !fileName.isEmpty())
                                    .collect(Collectors.joining(File.pathSeparator));

                            // Remove the classpath jar in question, and expand it with the files from the manifest
                            javaClasspath = Stream.of(javaClasspath.split(File.pathSeparator))
                                    .map(cp -> cp.matches(intellijClassPathJarRegex) ? filePaths : cp)
                                    .collect(Collectors.joining(File.pathSeparator));
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Error extract manifest file from " + javaClasspath + ".\n", e);
            }
        }
        return javaClasspath;
    }
}


