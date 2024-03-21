//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.DataDir;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.util.SynchronizedJavaFileManager;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.ByteUtils;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.CompletionStageFutureImpl;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryCompiler {

    private static final Logger log = LoggerFactory.getLogger(QueryCompiler.class);
    /**
     * We pick a number just shy of 65536, leaving a little elbow room for good luck.
     */
    private static final int DEFAULT_MAX_STRING_LITERAL_LENGTH = 65500;

    private static final String JAVA_CLASS_VERSION = System.getProperty("java.class.version").replace('.', '_');
    private static final int MAX_CLASS_COLLISIONS = 128;

    private static final String IDENTIFYING_FIELD_NAME = "_CLASS_BODY_";

    private static final String CODEGEN_TIMEOUT_PROP = "QueryCompiler.codegen.timeoutMs";
    private static final long CODEGEN_TIMEOUT_MS_DEFAULT = TimeUnit.SECONDS.toMillis(10); // 10 seconds
    private static final String CODEGEN_LOOP_DELAY_PROP = "QueryCompiler.codegen.retry.delay";
    private static final long CODEGEN_LOOP_DELAY_MS_DEFAULT = 100;
    private static final long CODEGEN_TIMEOUT_MS =
            Configuration.getInstance().getLongWithDefault(CODEGEN_TIMEOUT_PROP, CODEGEN_TIMEOUT_MS_DEFAULT);
    private static final long CODEGEN_LOOP_DELAY_MS =
            Configuration.getInstance().getLongWithDefault(CODEGEN_LOOP_DELAY_PROP, CODEGEN_LOOP_DELAY_MS_DEFAULT);

    private static boolean logEnabled = Configuration.getInstance().getBoolean("QueryCompiler.logEnabledDefault");

    public static final String FORMULA_PREFIX = "io.deephaven.temp";
    public static final String DYNAMIC_GROOVY_CLASS_PREFIX = "io.deephaven.dynamic";

    public static QueryCompiler create(File cacheDirectory, ClassLoader classLoader) {
        return new QueryCompiler(cacheDirectory, classLoader, true);
    }

    static QueryCompiler createForUnitTests() {
        final Path queryCompilerDir = DataDir.get()
                .resolve("io.deephaven.engine.context.QueryCompiler.createForUnitTests");
        return new QueryCompiler(queryCompilerDir.toFile());
    }

    private final Map<String, CompletionStageFuture<Class<?>>> knownClasses = new HashMap<>();

    private final String[] dynamicPatterns = new String[] {DYNAMIC_GROOVY_CLASS_PREFIX, FORMULA_PREFIX};

    private final File classDestination;
    private final boolean isCacheDirectory;
    private final Set<File> additionalClassLocations;
    private volatile WritableURLClassLoader ucl;

    /** package-private constructor for {@link io.deephaven.engine.context.PoisonedQueryCompiler} */
    QueryCompiler() {
        classDestination = null;
        isCacheDirectory = false;
        additionalClassLocations = null;
    }

    private QueryCompiler(File classDestination) {
        this(classDestination, null, false);
    }

    private QueryCompiler(
            final File classDestination,
            final ClassLoader parentClassLoader,
            final boolean isCacheDirectory) {
        final ClassLoader parentClassLoaderToUse = parentClassLoader == null
                ? QueryCompiler.class.getClassLoader()
                : parentClassLoader;
        this.classDestination = classDestination;
        this.isCacheDirectory = isCacheDirectory;
        ensureDirectories(this.classDestination, () -> "Failed to create missing class destination directory " +
                classDestination.getAbsolutePath());
        additionalClassLocations = new LinkedHashSet<>();

        URL[] urls = new URL[1];
        try {
            urls[0] = (classDestination.toURI().toURL());
        } catch (MalformedURLException e) {
            throw new UncheckedDeephavenException(e);
        }
        this.ucl = new WritableURLClassLoader(urls, parentClassLoaderToUse);

        if (isCacheDirectory) {
            addClassSource(classDestination);
        }
    }

    /**
     * Enables or disables compilation logging.
     *
     * @param logEnabled Whether logging should be enabled
     * @return The value of {@code logEnabled} before calling this method.
     */
    public static boolean setLogEnabled(boolean logEnabled) {
        boolean original = QueryCompiler.logEnabled;
        QueryCompiler.logEnabled = logEnabled;
        return original;
    }

    /*
     * NB: This is (obviously) not thread safe if code tries to write the same className to the same
     * destinationDirectory from multiple threads. Seeing as we don't currently have this use case, leaving
     * synchronization as an external concern.
     */
    public static void writeClass(final File destinationDirectory, final String className, final byte[] data)
            throws IOException {
        writeClass(destinationDirectory, className, data, null);
    }

    /*
     * NB: This is (obviously) not thread safe if code tries to write the same className to the same
     * destinationDirectory from multiple threads. Seeing as we don't currently have this use case, leaving
     * synchronization as an external concern.
     */
    public static void writeClass(final File destinationDirectory, final String className, final byte[] data,
            final String message) throws IOException {
        final File destinationFile = new File(destinationDirectory,
                className.replace('.', File.separatorChar) + JavaFileObject.Kind.CLASS.extension);

        if (destinationFile.exists()) {
            final byte[] existingBytes = Files.readAllBytes(destinationFile.toPath());
            if (Arrays.equals(existingBytes, data)) {
                if (message == null) {
                    log.info().append("Ignoring pushed class ").append(className)
                            .append(" because it already exists in this context!").endl();
                } else {
                    log.info().append("Ignoring pushed class ").append(className).append(message)
                            .append(" because it already exists in this context!").endl();
                }
                return;
            } else {
                if (message == null) {
                    log.info().append("Pushed class ").append(className)
                            .append(" already exists in this context, but has changed!").endl();
                } else {
                    log.info().append("Pushed class ").append(className).append(message)
                            .append(" already exists in this context, but has changed!").endl();
                }
                if (!destinationFile.delete()) {
                    throw new IOException("Could not delete existing class file: " + destinationFile);
                }
            }
        }

        final File parentDir = destinationFile.getParentFile();
        ensureDirectories(parentDir,
                () -> "Unable to create missing destination directory " + parentDir.getAbsolutePath());
        if (!destinationFile.createNewFile()) {
            throw new UncheckedDeephavenException(
                    "Unable to create destination file " + destinationFile.getAbsolutePath());
        }
        final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream(data.length);
        byteOutStream.write(data, 0, data.length);
        final FileOutputStream fileOutStream = new FileOutputStream(destinationFile);
        byteOutStream.writeTo(fileOutStream);
        fileOutStream.close();
    }

    public File getFakeClassDestination() {
        // Groovy classes need to be written out to a location where they can be found by the compiler
        // (so that filters and formulae can use them).
        //
        // We don't want the regular runtime class loader to find them, because then they get "stuck" in there
        // even if the class itself changes, and we can't forget it. So instead we use a single-use class loader
        // for each formula, that will always read the class from disk.
        return isCacheDirectory ? classDestination : null;
    }

    public void setParentClassLoader(final ClassLoader parentClassLoader) {
        // noinspection NonAtomicOperationOnVolatileField
        ucl = new WritableURLClassLoader(ucl.getURLs(), parentClassLoader);
    }

    /**
     * Compile a class.
     *
     * @param request The compilation request
     */
    public Class<?> compile(@NotNull final QueryCompilerRequest request) {
        final CompletionStageFuture.Resolver<Class<?>> resolver = CompletionStageFutureImpl.make();
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
    public void compile(
            @NotNull final QueryCompilerRequest request,
            @NotNull final CompletionStageFuture.Resolver<Class<?>> resolver) {
        // noinspection unchecked
        compile(new QueryCompilerRequest[] {request}, new CompletionStageFuture.Resolver[] {resolver});
    }

    /**
     * Compiles all requests.
     *
     * @param requests The compilation requests
     * @param resolvers The resolvers to use for delivering compilation results
     */
    public void compile(
            @NotNull final QueryCompilerRequest[] requests,
            @NotNull final CompletionStageFuture.Resolver<Class<?>>[] resolvers) {
        if (requests.length == 0) {
            return;
        }
        if (requests.length != resolvers.length) {
            throw new IllegalArgumentException("Requests and resolvers must be the same length");
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
                }
                /*
                 * RWC-CODE-REVIEW: If we inherit a compilation that's still in progress, is it possible for our batch
                 * to fail because our compilation units depend on one another, and hence depend on some other
                 * compilation that's not done yet? Maybe the solution to this is to document the concern in the JavaDoc
                 * since we don't use QueryCompiler in that way. That is, all of our compilations are independent, even
                 * within a batch.
                 */
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
                /*
                 * @formatter:off
                 * RWC-CODE-REVIEW: If this can happen, it's inappropriate to ever put a future into knownClasses for
                 * other threads to see until it's "done".
                 * By "done", I mean:
                 *   (a) It has been completed successfully, or
                 *   (b) it has been completed exceptionally with a failure that's determined to be isolated to the
                 *       particular classBody (e.g. because allFutures.size() == 1, and there was no interruption).
                 * If you don't follow this rule, with the current approach to sharing other threads may get() an
                 * inherited future and see the failure that you are "retracting" below.
                 *
                 * I see two options, here:
                 *   (1) Accept the possibility of duplicating efforts when compiling the same classBody as part of two
                 *       batches, and don't register in knownClasses until "done". If we go this route, we should
                 *       probably ensure we only load the "winner" of the putIfAbsent race for successful compilations.
                 *   (2) Allow for retries in case of inherited failures.
                 *       One implementation for this approach might be to have two flavors of sharing:
                 *         ( i) knownClasses for futures that are "done" or isolated (and hence will become "done), and
                 *         (ii) inProgressCompilations for compilations that might require retry.
                 *       You could basically execute the splitting of requests into "known", "inProgress", and "new"
                 *       followed by compiling "new" and get()ing "all" until you arrive at a state with no failures
                 *       inherited from "inProgress" compilations.
                 * @formatter:on
                 */
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
                resolvers[ii].complete(allFutures[ii].get());
            } catch (ExecutionException err) {
                resolvers[ii].completeExceptionally(err.getCause());
            } catch (Throwable err) {
                /*
                 * RWC-CODE-REVIEW: get() can throw InterruptedException, and you are catching it in your Throwable
                 * clause. Clearly, we never want any other thread to inherit an interrupted result. See note regarding
                 * sharing above. That said, I think we can safely assert that we only catch InterruptedException in a
                 * case where we *are* inheriting rather than sharing the future (either "known" or "inProgress"), hence
                 * this completeExceptionally() will not impact other threads.
                 */
                resolvers[ii].completeExceptionally(err);
            }
        }
    }

    private static void ensureDirectories(final File file, final Supplier<String> runtimeErrMsg) {
        // File.mkdirs() checks for existence on entry, in which case it returns false.
        // It may also return false on a failure to create.
        // Also note, two separate threads or JVMs may be running this code in parallel. It's possible that we could
        // lose the race
        // (and therefore mkdirs() would return false), but still get the directory we need (and therefore exists()
        // would return true)
        if (!file.mkdirs() && !file.isDirectory()) {
            throw new UncheckedDeephavenException(runtimeErrMsg.get());
        }
    }

    private ClassLoader getClassLoaderForFormula(final Map<String, Class<?>> parameterClasses) {
        return new URLClassLoader(ucl.getURLs(), ucl) {
            // Once we find a class that is missing, we should not attempt to load it again,
            // otherwise we can end up with a StackOverflow Exception
            final HashSet<String> missingClasses = new HashSet<>();

            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                // If we have a parameter that uses this class, return it
                final Class<?> paramClass = parameterClasses.get(name);
                if (paramClass != null) {
                    return paramClass;
                }

                // Unless we are looking for a formula or Groovy class, we should use the default behavior
                if (!isFormulaClass(name)) {
                    return super.findClass(name);
                }

                // if it is a groovy class, always try to use the instance in the shell
                if (name.startsWith(DYNAMIC_GROOVY_CLASS_PREFIX)) {
                    try {
                        return ucl.getParent().loadClass(name);
                    } catch (final ClassNotFoundException ignored) {
                        // we'll try to load it otherwise
                    }
                }

                // We've already not found this class, so we should not try to search again
                if (missingClasses.contains(name)) {
                    return super.findClass(name);
                }

                final byte[] bytes;
                try {
                    bytes = loadClassData(name);
                } catch (IOException ioe) {
                    missingClasses.add(name);
                    return super.loadClass(name);
                }
                return defineClass(name, bytes, 0, bytes.length);
            }

            @SuppressWarnings("BooleanMethodIsAlwaysInverted")
            private boolean isFormulaClass(String name) {
                return Arrays.stream(dynamicPatterns).anyMatch(name::startsWith);
            }

            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                if (!isFormulaClass(name)) {
                    return super.loadClass(name);
                }
                return findClass(name);
            }

            private byte[] loadClassData(String name) throws IOException {
                final File destFile = new File(classDestination,
                        name.replace('.', File.separatorChar) + JavaFileObject.Kind.CLASS.extension);
                if (destFile.exists()) {
                    return Files.readAllBytes(destFile.toPath());
                }

                for (File location : additionalClassLocations) {
                    final File checkFile = new File(location,
                            name.replace('.', File.separatorChar) + JavaFileObject.Kind.CLASS.extension);
                    if (checkFile.exists()) {
                        return Files.readAllBytes(checkFile.toPath());
                    }
                }

                throw new FileNotFoundException(name);
            }
        };
    }

    private static class WritableURLClassLoader extends URLClassLoader {
        private WritableURLClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            Class<?> clazz = findLoadedClass(name);
            if (clazz != null) {
                return clazz;
            }

            try {
                clazz = findClass(name);
            } catch (ClassNotFoundException e) {
                if (getParent() != null) {
                    clazz = getParent().loadClass(name);
                }
            }

            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        }

        @Override
        public synchronized void addURL(URL url) {
            super.addURL(url);
        }
    }

    private void addClassSource(File classSourceDirectory) {
        synchronized (additionalClassLocations) {
            if (additionalClassLocations.contains(classSourceDirectory)) {
                return;
            }
            additionalClassLocations.add(classSourceDirectory);
        }
        try {
            ucl.addURL(classSourceDirectory.toURI().toURL());
        } catch (MalformedURLException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    private File getClassDestination() {
        return classDestination;
    }

    private String getClassPath() {
        StringBuilder sb = new StringBuilder();
        sb.append(classDestination.getAbsolutePath());
        synchronized (additionalClassLocations) {
            for (File classLoc : additionalClassLocations) {
                sb.append(File.pathSeparatorChar).append(classLoc.getAbsolutePath());
            }
        }
        return sb.toString();
    }

    private static class CompilationState {
        int next_pi; // RWC-CODE-REVIEW: nextProbeIndex?
        boolean compiled;
        String packageName;
        String fqClassName;
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

        final String[] basicHashText = new String[requests.size()];
        for (int ii = 0; ii < requests.size(); ++ii) {
            basicHashText[ii] = ByteUtils.byteArrToHex(digest.digest(
                    requests.get(ii).classBody().getBytes(StandardCharsets.UTF_8)));
        }

        int numCompiled = 0;
        final CompilationState[] states = new CompilationState[requests.size()];
        for (int ii = 0; ii < requests.size(); ++ii) {
            states[ii] = new CompilationState();
        }

        /*
         * @formatter:off
         * 1. try to resolve without compiling; retain next hash to try
         * 2. compile all remaining with a single compilation task
         * 3. goto step 1 if any are unresolved
         * @formatter:on
         */

        while (numCompiled < requests.size()) {
            for (int ii = 0; ii < requests.size(); ++ii) {
                final CompilationState state = states[ii];
                if (state.compiled) {
                    continue;
                }

                while (true) {
                    final int pi = state.next_pi++;
                    final String packageNameSuffix = "c_" + basicHashText[ii]
                            + (pi == 0 ? "" : ("p" + pi))
                            + "v" + JAVA_CLASS_VERSION;

                    final QueryCompilerRequest request = requests.get(ii);
                    if (pi >= MAX_CLASS_COLLISIONS) {
                        /*
                         * RWC-CODE-REVIEW: If you get here, every un-compiled CompilationState has the same issue.
                         * Should this really be specific to just one request? Did we need the change to control flow?
                         */
                        Exception err = new IllegalStateException("Found too many collisions for package name root "
                                + request.packageNameRoot() + ", class name=" + request.className() + ", class body "
                                + "hash=" + basicHashText[ii] + " - contact Deephaven support!");
                        resolvers.get(ii).completeExceptionally(err);
                        state.compiled = true; /* RWC-CODE-REVIEW: Is this really compiled? Not in a useful way. */
                        ++numCompiled;
                        break;
                    }

                    state.packageName = request.getPackageName(packageNameSuffix);
                    state.fqClassName = state.packageName + "." + request.className();

                    // Ask the classloader to load an existing class with this name. This might:
                    // 1. Fail to find a class (returning null)
                    // 2. Find a class whose body has the formula we are looking for
                    // 3. Find a class whose body has a different formula (hash collision)
                    Class<?> result = tryLoadClassByFqName(state.fqClassName, request.parameterClasses());
                    if (result == null) {
                        break; // we'll try to compile it
                    }

                    if (completeIfResultMatchesQueryCompilerRequest(state.packageName, request, resolvers.get(ii),
                            result)) {
                        state.compiled = true;
                        ++numCompiled;
                        break;
                    }
                }
            }

            if (numCompiled == requests.size()) {
                return;
            }

            // Couldn't resolve at least one of the requests, so try a round of compilation.
            final List<CompilationRequestAttempt> compilationRequestAttempts = new ArrayList<>();
            for (int ii = 0; ii < requests.size(); ++ii) {
                final CompilationState state = states[ii];
                if (!state.compiled) {
                    final QueryCompilerRequest request = requests.get(ii);
                    compilationRequestAttempts.add(new CompilationRequestAttempt(
                            request,
                            state.packageName,
                            state.fqClassName,
                            resolvers.get(ii)));
                }
            }

            maybeCreateClasses(compilationRequestAttempts);

            // We could be running on a screwy filesystem that is slow (e.g. NFS). If we wrote a file and can't load it
            // ... then give the filesystem some time. All requests should use the same deadline.
            final long deadline = System.currentTimeMillis() + CODEGEN_TIMEOUT_MS - CODEGEN_LOOP_DELAY_MS;
            for (int ii = 0; ii < requests.size(); ++ii) {
                final CompilationState state = states[ii];
                if (state.compiled) {
                    continue;
                }

                final QueryCompilerRequest request = requests.get(ii);
                final CompletionStageFuture.Resolver<Class<?>> resolver = resolvers.get(ii);
                if (resolver.getFuture().isDone()) {
                    state.compiled = true;
                    ++numCompiled;
                    continue;
                }

                // This request may have:
                // A. succeeded
                // B. Lost a race to another process on the same file system which is compiling the identical formula
                // C. Lost a race to another process on the same file system compiling a different formula that collides

                Class<?> clazz = tryLoadClassByFqName(state.fqClassName, request.parameterClasses());
                try {
                    while (clazz == null && System.currentTimeMillis() < deadline) {
                        // noinspection BusyWait
                        Thread.sleep(CODEGEN_LOOP_DELAY_MS);
                        clazz = tryLoadClassByFqName(state.fqClassName, request.parameterClasses());
                    }
                } catch (final InterruptedException ie) {
                    throw new UncheckedDeephavenException("Interrupted while waiting for codegen", ie);
                }

                // However, regardless of A-C, there will be *some* class being found
                if (clazz == null) {
                    throw new IllegalStateException("Should have been able to load *some* class here");
                }

                if (completeIfResultMatchesQueryCompilerRequest(state.packageName, request, resolver, clazz)) {
                    state.compiled = true;
                    ++numCompiled;
                }
            }
        }
    }

    private boolean completeIfResultMatchesQueryCompilerRequest(
            final String packageName,
            final QueryCompilerRequest request,
            final CompletionStageFuture.Resolver<Class<?>> resolver,
            final Class<?> result) {
        final String identifyingFieldValue = loadIdentifyingField(result);
        if (!request.classBody().equals(identifyingFieldValue)) {
            return false;
        }

        // If the caller wants a textual copy of the code we either made, or just found in the cache.
        request.codeLog()
                .ifPresent(sb -> sb.append(makeFinalCode(request.className(), request.classBody(), packageName)));

        // If the class we found was indeed the class we were looking for, then complete the future and return it.
        resolver.complete(result);

        synchronized (this) {
            // Note we are doing something kind of subtle here. We are removing an entry whose key was matched
            // by value equality and replacing it with a value-equal but reference-different string that is a
            // static member of the class we just loaded. This should be easier on the garbage collector because
            // we are replacing a calculated value with a classloaded value and so in effect we are
            // "canonicalizing" the string. This is important because these long strings stay in knownClasses
            // forever.
            knownClasses.remove(identifyingFieldValue);
            knownClasses.put(identifyingFieldValue, resolver.getFuture());
        }

        return true;
    }

    private Class<?> tryLoadClassByFqName(String fqClassName, Map<String, Class<?>> parameterClasses) {
        try {
            return getClassLoaderForFormula(parameterClasses).loadClass(fqClassName);
        } catch (ClassNotFoundException cnfe) {
            return null;
        }
    }

    private static String loadIdentifyingField(Class<?> c) {
        try {
            final Field field = c.getDeclaredField(IDENTIFYING_FIELD_NAME);
            return (String) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Malformed class in cache", e);
        }
    }

    private static String makeFinalCode(String className, String classBody, String packageName) {
        final String joinedEscapedBody = createEscapedJoinedString(classBody);
        classBody = classBody.replaceAll("\\$CLASSNAME\\$", className);
        classBody = classBody.substring(0, classBody.lastIndexOf("}"));
        classBody += "    public static String " + IDENTIFYING_FIELD_NAME + " = " + joinedEscapedBody + ";\n}";
        return "package " + packageName + ";\n" + classBody;
    }

    /**
     * Transform a string into the corresponding Java source code that compiles into that string. This involves escaping
     * special characters, surrounding it with quotes, and (if the string is larger than the max string length for Java
     * literals), splitting it into substrings and constructing a call to String.join() that combines those substrings.
     */
    public static String createEscapedJoinedString(final String originalString) {
        return createEscapedJoinedString(originalString, DEFAULT_MAX_STRING_LITERAL_LENGTH);
    }

    public static String createEscapedJoinedString(final String originalString, int maxStringLength) {
        final String[] splits = splitByModifiedUtf8Encoding(originalString, maxStringLength);

        // Turn each split into a Java source string by escaping it and surrounding it with "
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
        // exclusive end position of the previous substring.
        int previousEnd = 0;
        // Number of bytes in the "modified UTF-8" representation of the substring we are currently scanning.
        int currentByteCount = 0;
        for (int ii = 0; ii < originalString.length(); ++ii) {
            final int bytesConsumed = calcBytesConsumed(originalString.charAt(ii));
            if (currentByteCount + bytesConsumed > maxBytes) {
                // This character won't fit in this string, so we flush the buffer.
                splits.add(originalString.substring(previousEnd, ii));
                previousEnd = ii;
                currentByteCount = 0;
            }
            currentByteCount += bytesConsumed;
        }
        // At the end of the loop, either
        // 1. there are one or more characters that still need to be added to splits
        // 2. originalString was empty and so splits is empty and we need to add a single empty string to splits
        splits.add(originalString.substring(previousEnd));
        return splits.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
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

        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private static class CompilationRequestAttempt {
        final String description;
        final String fqClassName;
        final String finalCode;
        final String packageName;
        final String[] splitPackageName;
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

            splitPackageName = packageName.split("\\.");
            if (splitPackageName.length == 0) {
                final Exception err = new UncheckedDeephavenException(String.format(
                        "packageName %s expected to have at least one .", packageName));
                resolver.completeExceptionally(err);
            }
        }

        public void ensureDirectories(@NotNull final String rootPath) {
            if (splitPackageName.length == 0) {
                // we've already failed
                return;
            }

            final String[] truncatedSplitPackageName = Arrays.copyOf(splitPackageName, splitPackageName.length - 1);
            final Path rootPathWithPackage = Paths.get(rootPath, truncatedSplitPackageName);
            final File rpf = rootPathWithPackage.toFile();
            QueryCompiler.ensureDirectories(rpf, () -> "Couldn't create package directories: " + rootPathWithPackage);
        }

        public JavaSourceFromString makeSource() {
            return new JavaSourceFromString(description, fqClassName, finalCode, resolver);
        }
    }

    private void maybeCreateClasses(
            @NotNull final List<CompilationRequestAttempt> requests) {
        // Get the destination root directory (e.g. /tmp/workspace/cache/classes) and populate it with the package
        // directories (e.g. io/deephaven/test) if they are not already there. This will be useful later.
        // Also create a temp directory e.g. /tmp/workspace/cache/classes/temporaryCompilationDirectory12345
        // This temp directory will be where the compiler drops files into, e.g.
        // /tmp/workspace/cache/classes/temporaryCompilationDirectory12345/io/deephaven/test/cm12862183232603186v52_0/Formula.class
        // Foreshadowing: we will eventually atomically move cm12862183232603186v52_0 from the above to
        // /tmp/workspace/cache/classes/io/deephaven/test
        // Note: for this atomic move to work, this temp directory must be on the same file system as the destination
        // directory.
        final String rootPathAsString;
        final String tempDirAsString;
        try {
            rootPathAsString = getClassDestination().getAbsolutePath();
            final Path tempPath =
                    Files.createTempDirectory(Paths.get(rootPathAsString), "temporaryCompilationDirectory");
            tempDirAsString = tempPath.toFile().getAbsolutePath();

            for (final CompilationRequestAttempt request : requests) {
                request.ensureDirectories(rootPathAsString);
            }
        } catch (IOException ioe) {
            Exception err = new UncheckedIOException(ioe);
            for (final CompilationRequestAttempt request : requests) {
                request.resolver.completeExceptionally(err);
            }
            return;
        }


        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new UncheckedDeephavenException("No Java compiler provided - are you using a JRE instead of a JDK?");
        }

        /* RWC-CODE-REVIEW: Did it turn out that caching file managers was a dead end, feature-loss-wise? */
        final JavaFileManager fileManager = new SynchronizedJavaFileManager(
                compiler.getStandardFileManager(null, null, null));

        boolean exceptionCaught = false;
        try {
            final OperationInitializer operationInitializer = ExecutionContext.getContext().getOperationInitializer();
            int parallelismFactor = operationInitializer.parallelismFactor();

            int requestsPerTask = Math.max(32, (requests.size() + parallelismFactor - 1) / parallelismFactor);
            /* RWC-CODE-REVIEW: Should this be debug level? */
            log.info().append("Compiling with parallelismFactor = ").append(parallelismFactor)
                    .append(" requestsPerTask = ").append(requestsPerTask).endl();
            if (parallelismFactor == 1 || requestsPerTask >= requests.size()) {
                maybeCreateClassHelper(compiler, fileManager, requests, rootPathAsString, tempDirAsString,
                        0, requests.size());
            } else {
                int numTasks = (requests.size() + requestsPerTask - 1) / requestsPerTask;
                final Future<?>[] tasks = new Future[numTasks];
                for (int jobId = 0; jobId < numTasks; ++jobId) {
                    final int startInclusive = jobId * requestsPerTask;
                    final int endExclusive = Math.min(requests.size(), (jobId + 1) * requestsPerTask);
                    tasks[jobId] = operationInitializer.submit(() -> {
                        maybeCreateClassHelper(compiler, fileManager, requests, rootPathAsString, tempDirAsString,
                                startInclusive, endExclusive);
                    });
                }
                for (int jobId = 0; jobId < numTasks; ++jobId) {
                    try {
                        tasks[jobId].get();
                    } catch (Exception err) {
                        throw new UncheckedDeephavenException("Exception waiting for compilation task", err);
                    }
                }
            }
        } catch (final Throwable t) {
            exceptionCaught = true;
            throw t;
        } finally {
            try {
                /*
                 * RWC-CODE-REVIEW: I think it would be desirable if you waited for all the futures to finish before
                 * trying to delete their target dir out from under them. I just worry about leaving things in a weird
                 * state. Also, we hate leaving jobs running in the thread pool. Maybe you should use IterationManager,
                 * which handles this waiting.
                 */
                FileUtils.deleteRecursively(new File(tempDirAsString));
            } catch (Exception e) {
                // ignore errors here
            }

            try {
                /* RWC-CODE-REVIEW: Same thing, re: the waiting. You might close out from under a running compile. */
                fileManager.close();
            } catch (IOException ioe) {
                if (!exceptionCaught) {
                    // noinspection ThrowFromFinallyBlock
                    throw new UncheckedIOException("Could not close JavaFileManager", ioe);
                }
            }
        }
    }

    private void maybeCreateClassHelper( /* RWC-CODE-REVIEW: Does this name still make sense? */
            @NotNull final JavaCompiler compiler,
            @NotNull final JavaFileManager fileManager,
            @NotNull final List<CompilationRequestAttempt> requests,
            @NotNull final String rootPathAsString,
            @NotNull final String tempDirAsString,
            final int startInclusive,
            final int endExclusive) {
        final List<CompilationRequestAttempt> toRetry = new ArrayList<>();
        final boolean wantRetry = maybeCreateClassHelper2(compiler,
                fileManager, requests, rootPathAsString, tempDirAsString, startInclusive, endExclusive, toRetry);
        if (!wantRetry) {
            return;
        }

        final List<CompilationRequestAttempt> ignored = new ArrayList<>();
        if (maybeCreateClassHelper2(compiler,
                fileManager, toRetry, rootPathAsString, tempDirAsString, 0, toRetry.size(), ignored)) {
            // We only retried compilation units that did not fail on the first pass, so we should not have any failures
            // on the second pass.
            throw new IllegalStateException("Unexpected failure during second pass of compilation");
        }
    }

    private boolean maybeCreateClassHelper2(
            @NotNull final JavaCompiler compiler,
            @NotNull final JavaFileManager fileManager,
            @NotNull final List<CompilationRequestAttempt> requests,
            @NotNull final String rootPathAsString,
            @NotNull final String tempDirAsString,
            final int startInclusive,
            final int endExclusive,
            List<CompilationRequestAttempt> toRetry) {
        final StringWriter compilerOutput = new StringWriter();

        final String classPathAsString = getClassPath() + File.pathSeparator + getJavaClassPath();
        final List<String> compilerOptions = Arrays.asList(
                "-d", tempDirAsString,
                "-cp", classPathAsString,
                // this option allows the compiler to attempt to process all source files even if some of them fail
                "--should-stop=ifError=GENERATE");

        final MutableInt numFailures = new MutableInt(0);
        compiler.getTask(compilerOutput,
                fileManager,
                diagnostic -> {
                    if (diagnostic.getKind() != Diagnostic.Kind.ERROR) {
                        return;
                    }

                    final JavaSourceFromString source = (JavaSourceFromString) diagnostic.getSource();
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

        /*
         * RWC-CODE-REVIEW: Why do we ever want to retry? Seems like move collisions. If so, why do we only retry one
         * time? Seems user-unfriendly.
         */
        final boolean wantRetry = numFailures.intValue() > 0 && numFailures.intValue() != endExclusive - startInclusive;

        // The above has compiled into e.g.
        // /tmp/workspace/cache/classes/temporaryCompilationDirectory12345/io/deephaven/test/cm12862183232603186v52_0/{various
        // class files}
        // We want to atomically move it to e.g.
        // /tmp/workspace/cache/classes/io/deephaven/test/cm12862183232603186v52_0/{various class files}
        requests.subList(startInclusive, endExclusive).forEach(request -> {
            final Path srcDir = Paths.get(tempDirAsString, request.splitPackageName);
            final Path destDir = Paths.get(rootPathAsString, request.splitPackageName);
            try {
                Files.move(srcDir, destDir, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException ioe) {
                // The name "isDone" might be misleading here. We haven't called "complete" on the successful
                // futures yet, so the only way they would be "done" at this point is if they completed
                // exceptionally.
                final boolean hasException = request.resolver.getFuture().isDone();

                if (wantRetry && !Files.exists(srcDir)) {
                    // The move failed and the source directory does not exist.
                    if (!hasException) {
                        // This source actually succeeded in compiling, but was not written because some other source
                        // failed to compile. Let's schedule this work to try again.
                        toRetry.add(request);
                        return;
                    }
                }

                if (!Files.exists(destDir) && !hasException) {
                    // Propagate an error here only if the destination does not exist; ignoring issues related to
                    // collisions with another process.
                    request.resolver.completeExceptionally(new UncheckedIOException(
                            "Move failed for some reason other than destination already existing", ioe));
                }
            }
        });

        return wantRetry;
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
        // class path
        // Look for this being used during compile time, so the full class path can be sent into the compile call
        final String intellijClassPathJarRegex = ".*classpath[0-9]*\\.jar.*";
        if (javaClasspath.matches(intellijClassPathJarRegex)) {
            try {
                final Enumeration<URL> resources =
                        QueryCompiler.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
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
                            // and
                            // use the default path separator
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
