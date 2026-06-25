//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

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
import io.deephaven.util.ByteUtils;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.mutable.MutableInt;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
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
 * provided parent classloader, enabling GC of both classes and classloaders when neither the compiled Class nor this
 * compiler instance are reachable.
 *
 * <h2>Constraints</h2>
 * <ul>
 * <li>Compiled classes must not depend on other classes compiled by this or any other QueryCompiler instance.</li>
 * <li>The caller must ensure the parent classloader already contains any classes referenced by compiled formulas (e.g.,
 * Groovy-defined classes).</li>
 * <li>Each compilation request produces a single top-level class (matching {@link QueryCompilerRequest#className()})
 * which may contain static inner classes and anonymous classes.</li>
 * </ul>
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
    public static QueryCompilerImpl create(@Nullable final File additionalClassPathDir) {
        return new QueryCompilerImpl(additionalClassPathDir, null);
    }

    public static QueryCompilerImpl createForUnitTests() {
        return createForUnitTests(null);
    }

    static QueryCompilerImpl createForUnitTests(final List<String> classNamesForAnnotationProcessing) {
        return new QueryCompilerImpl(null, classNamesForAnnotationProcessing);
    }

    private final Map<String, CompletionStageFuture<Class<?>>> knownClasses = new HashMap<>();

    /** Set of fully-qualified class names already assigned, for collision avoidance. */
    private final Set<String> takenNames = new HashSet<>();

    /** The context classloader captured at construction time; used as parent for per-batch classloaders. */
    private final ClassLoader parentClassLoader;

    /** Optional additional classpath directory (e.g., where Groovy writes bytecode). */
    @Nullable
    private final File additionalClassPathDir;

    // This is for test use only, specifying a non-null list causes an error without a specific source to be generated.
    private final List<String> classNamesForAnnotationProcessing;

    private QueryCompilerImpl(
            @Nullable final File additionalClassPathDir,
            final List<String> classNamesForAnnotationProcessing) {
        ensureJavaCompiler();
        this.additionalClassPathDir = additionalClassPathDir;
        this.parentClassLoader = Thread.currentThread().getContextClassLoader();
        this.classNamesForAnnotationProcessing = classNamesForAnnotationProcessing;

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

        // noinspection unchecked
        final CompletionStageFuture<Class<?>>[] allFutures = new CompletionStageFuture[requests.length];

        final List<QueryCompilerRequest> newRequests = new ArrayList<>();
        final List<CompletionStageFuture.Resolver<Class<?>>> newResolvers = new ArrayList<>();

        synchronized (this) {
            for (int ii = 0; ii < requests.length; ++ii) {
                final QueryCompilerRequest request = requests[ii];
                final CompletionStageFuture.Resolver<Class<?>> resolver = resolvers[ii];

                CompletionStageFuture<Class<?>> future =
                        knownClasses.putIfAbsent(request.classBody(), resolver.getFuture());
                if (future == null) {
                    newRequests.add(request);
                    newResolvers.add(resolver);
                    future = resolver.getFuture();
                } else if (shouldTrace) {
                    log.trace().append("Found existing future in knownClasses for ").append(request.className())
                            .append(" (done=").append(future.isDone()).append(")").endl();
                }
                allFutures[ii] = future;
            }
        }

        if (!newResolvers.isEmpty()) {
            // It's my job to fulfill these futures.
            try {
                compileHelper(newRequests, newResolvers);
            } catch (RuntimeException e) {
                // These failures are not applicable to a single request, so we can't just complete the future and
                // leave the failure in the cache.
                synchronized (this) {
                    for (int ii = 0; ii < newRequests.size(); ++ii) {
                        if (newResolvers.get(ii).completeExceptionally(e)) {
                            knownClasses.remove(newRequests.get(ii).classBody());
                        }
                    }
                }
                throw e;
            }
        }

        for (int ii = 0; ii < requests.length; ++ii) {
            try {
                final CompletionStageFuture<Class<?>> future = allFutures[ii];
                // TODO do we want a timeout here, knowing that this might leave other bad state?
                resolvers[ii].complete(future.get(10, TimeUnit.SECONDS));
            } catch (TimeoutException err) {
                final String msg = "Timed out (10s) waiting for class compilation"
                        + " request[" + ii + "] className=" + requests[ii].className()
                        + " future=" + allFutures[ii]
                        + " isDone=" + allFutures[ii].isDone();
                log.error().append(msg).endl();
                // Fail all remaining resolvers and throw immediately
                final UncheckedDeephavenException timeout = new UncheckedDeephavenException(msg);
                for (int jj = ii; jj < requests.length; ++jj) {
                    resolvers[jj].completeExceptionally(timeout);
                }
                throw timeout;
            } catch (ExecutionException err) {
                resolvers[ii].completeExceptionally(err.getCause());
            } catch (InterruptedException err) {
                // This can only occur if we are interrupted while waiting for the future to complete from another
                // compilation request.
                Assert.notEquals(resolvers[ii], "resolvers[ii]", allFutures[ii], "allFutures[ii]");
                resolvers[ii].completeExceptionally(err);
            } catch (Throwable err) {
                resolvers[ii].completeExceptionally(err);
            }
        }
    }

    private void compileHelper(
            @NotNull final List<QueryCompilerRequest> requests,
            @NotNull final List<CompletionStageFuture.Resolver<Class<?>>> resolvers) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new UncheckedDeephavenException("Unable to create SHA-256 hashing digest", e);
        }

        // Assign unique FQ class names for each request
        final String[] fqClassNames = new String[requests.size()];
        final String[] packageNames = new String[requests.size()];

        synchronized (this) {
            for (int ii = 0; ii < requests.size(); ++ii) {
                final QueryCompilerRequest request = requests.get(ii);
                final String hashText = ByteUtils.byteArrToHex(digest.digest(
                        request.classBody().getBytes(StandardCharsets.UTF_8)));

                String fqClassName = null;
                String packageName = null;
                for (int pi = 0; pi < 128; ++pi) {
                    final String packageNameSuffix = "c_" + hashText
                            + (pi == 0 ? "" : ("p" + pi))
                            + "v" + JAVA_CLASS_VERSION;
                    packageName = request.getPackageName(packageNameSuffix);
                    fqClassName = packageName + "." + request.className();
                    if (!takenNames.contains(fqClassName)) {
                        break;
                    }
                    fqClassName = null;
                }
                if (fqClassName == null) {
                    resolvers.get(ii).completeExceptionally(new IllegalStateException(
                            "Unable to assign unique class name for " + request.className()));
                    continue;
                }
                takenNames.add(fqClassName);
                fqClassNames[ii] = fqClassName;
                packageNames[ii] = packageName;
            }
        }

        // Build compilation attempts for requests that got a valid name
        final List<CompilationRequestAttempt> attempts = new ArrayList<>();
        for (int ii = 0; ii < requests.size(); ++ii) {
            if (fqClassNames[ii] == null) {
                continue; // already failed
            }
            attempts.add(new CompilationRequestAttempt(
                    requests.get(ii), packageNames[ii], fqClassNames[ii], resolvers.get(ii)));
        }

        if (attempts.isEmpty()) {
            return;
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
            // TODO Probably should be configurable, probably shouldn't be indefinite?
            //      Allowing timeout like this may require cleanup
            final boolean completed = latch.await(10, TimeUnit.SECONDS);
            if (!completed) {
                final String msg = "QueryCompilerImpl.compileAndDefine: latch timed out after 10s!"
                        + " numTasks=" + numTasks + " requests=" + requests.size();
                log.error().append(msg).endl();
                throw new UncheckedDeephavenException(msg);
            }
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
                classNamesForAnnotationProcessing,
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
        log.info().append("compilation produced ").append(compiledClasses.size())
                .append(" classes, numFailures=").append(numFailures.get())
                .append(", range=[").append(startInclusive).append(",").append(endExclusive).append(")")
                .endl();
        if (!compiledClasses.isEmpty()) {
            // Use the current thread's context classloader as parent so that Groovy-defined classes
            // (which may have been loaded after this QueryCompilerImpl was constructed) are visible.
            final ClassLoader currentContextCl = Thread.currentThread().getContextClassLoader();
            final ClassLoader batchParent = currentContextCl != null ? currentContextCl : parentClassLoader;
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

                // Load the top-level class (which triggers loading of inner/anonymous classes as needed)
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
                log.info().append("Resolving ").append(request.fqClassName).endl();
                request.resolver.complete(clazz);

                // Canonicalize the knownClasses entry
                synchronized (this) {
                    knownClasses.remove(identifyingFieldValue);
                    knownClasses.put(identifyingFieldValue, request.resolver.getFuture());
                }
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
            final InMemoryClassFileObject fileObject = new InMemoryClassFileObject(className);
            outputClasses.put(className, fileObject);
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

    static String makeFinalCode(String className, String classBody, String packageName) {
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


