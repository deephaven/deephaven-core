/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.base.Pair;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
    private static final long codegenTimeoutMs =
            Configuration.getInstance().getLongWithDefault(CODEGEN_TIMEOUT_PROP, CODEGEN_TIMEOUT_MS_DEFAULT);
    private static final long codegenLoopDelayMs =
            Configuration.getInstance().getLongWithDefault(CODEGEN_LOOP_DELAY_PROP, CODEGEN_LOOP_DELAY_MS_DEFAULT);

    private static boolean logEnabled = Configuration.getInstance().getBoolean("QueryCompiler.logEnabledDefault");

    public static final String FORMULA_PREFIX = "io.deephaven.temp";
    public static final String DYNAMIC_GROOVY_CLASS_PREFIX = "io.deephaven.dynamic";

    public static QueryCompiler create(File cacheDirectory, ClassLoader classLoader) {
        return new QueryCompiler(cacheDirectory, classLoader, true);
    }

    static QueryCompiler createForUnitTests() {
        return new QueryCompiler(new File(Configuration.getInstance().getWorkspacePath() +
                File.separator + "cache" + File.separator + "classes"));
    }

    private final Map<String, CompletableFuture<Class<?>>> knownClasses = new HashMap<>();

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
            throw new RuntimeException("", e);
        }
        this.ucl = new WritableURLClassLoader(urls, parentClassLoaderToUse);

        if (isCacheDirectory) {
            addClassSource(classDestination);
        }
    }

    /**
     * Enables or disables compilation logging.
     *
     * @param logEnabled Whether or not logging should be enabled
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
            throw new RuntimeException("Unable to create destination file " + destinationFile.getAbsolutePath());
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
        ucl = new WritableURLClassLoader(ucl.getURLs(), parentClassLoader);
    }

    public final Class<?> compile(String className, String classBody, String packageNameRoot) {
        return compile(className, classBody, packageNameRoot, null, Collections.emptyMap());
    }

    public final Class<?> compile(String className, String classBody, String packageNameRoot,
            Map<String, Class<?>> parameterClasses) {
        return compile(className, classBody, packageNameRoot, null, parameterClasses);
    }

    public final Class<?> compile(String className, String classBody, String packageNameRoot, StringBuilder codeLog) {
        return compile(className, classBody, packageNameRoot, codeLog, Collections.emptyMap());
    }

    /**
     * Compile a class.
     *
     * @param className Class name
     * @param classBody Class body, before update with "$CLASS_NAME$" replacement and package name prefixing
     * @param packageNameRoot Package name prefix
     * @param codeLog Optional "log" for final class code
     * @param parameterClasses Generic parameters, empty if none required
     * @return The compiled class
     */
    public Class<?> compile(@NotNull final String className,
            @NotNull final String classBody,
            @NotNull final String packageNameRoot,
            @Nullable final StringBuilder codeLog,
            @NotNull final Map<String, Class<?>> parameterClasses) {
        CompletableFuture<Class<?>> promise;
        final boolean promiseAlreadyMade;

        synchronized (this) {
            promise = knownClasses.get(classBody);
            if (promise != null) {
                promiseAlreadyMade = true;
            } else {
                promise = new CompletableFuture<>();
                knownClasses.put(classBody, promise);
                promiseAlreadyMade = false;
            }
        }

        // Someone else has already made the promise. I'll just wait for the answer.
        if (promiseAlreadyMade) {
            try {
                return promise.get();
            } catch (InterruptedException | ExecutionException error) {
                throw new UncheckedDeephavenException(error);
            }
        }

        // It's my job to fulfill the promise
        try {
            return compileHelper(className, classBody, packageNameRoot, codeLog, parameterClasses);
        } catch (RuntimeException e) {
            promise.completeExceptionally(e);
            throw e;
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
            throw new RuntimeException(runtimeErrMsg.get());
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
            throw new RuntimeException("", e);
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

    private WritableURLClassLoader getClassLoader() {
        return ucl;
    }

    private Class<?> compileHelper(@NotNull final String className,
            @NotNull final String classBody,
            @NotNull final String packageNameRoot,
            @Nullable final StringBuilder codeLog,
            @NotNull final Map<String, Class<?>> parameterClasses) {
        // NB: We include class name hash in order to (hopefully) account for case insensitive file systems.
        final int classNameHash = className.hashCode();
        final int classBodyHash = classBody.hashCode();

        for (int pi = 0; pi < MAX_CLASS_COLLISIONS; ++pi) {
            final String packageNameSuffix = "c"
                    + (classBodyHash < 0 ? "m" : "") + (classBodyHash & Integer.MAX_VALUE)
                    + (classNameHash < 0 ? "n" : "") + (classNameHash & Integer.MAX_VALUE)
                    + (pi == 0 ? "" : ("p" + pi))
                    + "v" + JAVA_CLASS_VERSION;
            final String packageName = (packageNameRoot.isEmpty()
                    ? packageNameSuffix
                    : packageNameRoot + (packageNameRoot.endsWith(".") ? "" : ".") + packageNameSuffix);
            final String fqClassName = packageName + "." + className;

            // Ask the classloader to load an existing class with this name. This might:
            // 1. Fail to find a class (returning null)
            // 2. Find a class whose body has the formula we are looking for
            // 3. Find a class whose body has a different formula (hash collision)
            Class<?> result = tryLoadClassByFqName(fqClassName, parameterClasses);
            if (result == null) {
                // Couldn't find one, so try to create it. This might:
                // A. succeed
                // B. Lose a race to another process on the same file system which is compiling the identical formula
                // C. Lose a race to another process on the same file system compiling a different formula that
                // happens to have the same hash (same packageName).
                // However, regardless of A-C, there will be *some* class being found (i.e. tryLoadClassByFqName won't
                // return null).
                maybeCreateClass(className, classBody, packageName, fqClassName);

                // We could be running on a screwy filesystem that is slow (e.g. NFS).
                // If we wrote a file and can't load it ... then give the filesystem some time.
                result = tryLoadClassByFqName(fqClassName, parameterClasses);
                try {
                    final long deadline = System.currentTimeMillis() + codegenTimeoutMs - codegenLoopDelayMs;
                    while (result == null && System.currentTimeMillis() < deadline) {
                        Thread.sleep(codegenLoopDelayMs);
                        result = tryLoadClassByFqName(fqClassName, parameterClasses);
                    }
                } catch (InterruptedException ignored) {
                    // we got interrupted, just quit looping and ignore it.
                }

                if (result == null) {
                    throw new IllegalStateException("Should have been able to load *some* class here");
                }
            }

            final String identifyingFieldValue = loadIdentifyingField(result);

            // We have a class. It either contains the formula we are looking for (cases 2, A, and B) or a different
            // formula with the same name (cases 3 and C). In either case, we should store the result in our cache,
            // either fulfilling an existing promise or making a new, fulfilled promise.
            synchronized (this) {
                // Note we are doing something kind of subtle here. We are removing an entry whose key was matched by
                // value equality and replacing it with a value-equal but reference-different string that is a static
                // member of the class we just loaded. This should be easier on the garbage collector because we are
                // replacing a calculated value with a classloaded value and so in effect we are "canonicalizing" the
                // string. This is important because these long strings stay in knownClasses forever.
                CompletableFuture<Class<?>> p = knownClasses.remove(identifyingFieldValue);
                if (p == null) {
                    // If we encountered a different class than the one we're looking for, make a fresh promise and
                    // immediately fulfill it. This is for the purpose of populating the cache in case someone comes
                    // looking for that class later. Rationale: we already did all the classloading work; no point in
                    // throwing it away now, even though this is not the class we're looking for.
                    p = new CompletableFuture<>();
                }
                knownClasses.put(identifyingFieldValue, p);
                // It's also possible that some other code has already fulfilled this promise with exactly the same
                // class. That's ok though: the promise code does not reject multiple sets to the identical value.
                p.complete(result);
            }

            // If the class we found was indeed the class we were looking for, then return it
            if (classBody.equals(identifyingFieldValue)) {
                // Cases 2, A, and B.
                if (codeLog != null) {
                    // If the caller wants a textual copy of the code we either made, or just found in the cache.
                    codeLog.append(makeFinalCode(className, classBody, packageName));
                }
                return result;
            }
            // Try the next hash name
        }
        throw new IllegalStateException("Found too many collisions for package name root " + packageNameRoot
                + ", class name=" + className
                + ", class body hash=" + classBodyHash + " - contact Deephaven support!");
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
        final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private static class JavaSourceFromFile extends SimpleJavaFileObject {
        private static final int JAVA_LENGTH = Kind.SOURCE.extension.length();
        final String code;

        private JavaSourceFromFile(File basePath, File file) {
            super(URI.create("string:///" + createName(basePath, file).replace('.', '/') + Kind.SOURCE.extension),
                    Kind.SOURCE);
            try {
                this.code = FileUtils.readTextFile(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static String createName(File basePath, File file) {
            final String base = basePath.getAbsolutePath();
            final String fileName = file.getAbsolutePath();
            if (!fileName.startsWith(base)) {
                throw new IllegalArgumentException(file + " is not in " + basePath);
            }
            final String basename = fileName.substring(base.length());
            if (basename.endsWith(".java")) {
                return basename.substring(0, basename.length() - JAVA_LENGTH);
            } else {
                return basename;
            }
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private void maybeCreateClass(String className, String code, String packageName, String fqClassName) {
        final String finalCode = makeFinalCode(className, code, packageName);

        if (logEnabled) {
            log.info().append("Generating code ").append(finalCode).endl();
        }

        final File ctxClassDestination = getClassDestination();

        final String[] splitPackageName = packageName.split("\\.");
        if (splitPackageName.length == 0) {
            throw new RuntimeException(String.format("packageName %s expected to have at least one .", packageName));
        }
        final String[] truncatedSplitPackageName = Arrays.copyOf(splitPackageName, splitPackageName.length - 1);

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
            rootPathAsString = ctxClassDestination.getAbsolutePath();
            final Path rootPathWithPackage = Paths.get(rootPathAsString, truncatedSplitPackageName);
            final File rpf = rootPathWithPackage.toFile();
            ensureDirectories(rpf, () -> "Couldn't create package directories: " + rootPathWithPackage);
            final Path tempPath =
                    Files.createTempDirectory(Paths.get(rootPathAsString), "temporaryCompilationDirectory");
            tempDirAsString = tempPath.toFile().getAbsolutePath();
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

        try {
            maybeCreateClassHelper(fqClassName, finalCode, splitPackageName, rootPathAsString, tempDirAsString);
        } finally {
            try {
                FileUtils.deleteRecursively(new File(tempDirAsString));
            } catch (Exception e) {
                // ignore errors here
            }
        }
    }

    private void maybeCreateClassHelper(String fqClassName, String finalCode, String[] splitPackageName,
            String rootPathAsString, String tempDirAsString) {
        final StringWriter compilerOutput = new StringWriter();

        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new RuntimeException("No Java compiler provided - are you using a JRE instead of a JDK?");
        }

        final String classPathAsString = getClassPath() + File.pathSeparator + getJavaClassPath();
        final List<String> compilerOptions = Arrays.asList("-d", tempDirAsString, "-cp", classPathAsString);

        final StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);

        final boolean result = compiler.getTask(compilerOutput,
                fileManager,
                null,
                compilerOptions,
                null,
                Collections.singletonList(new JavaSourceFromString(fqClassName, finalCode)))
                .call();
        if (!result) {
            throw new RuntimeException("Error compiling class " + fqClassName + ":\n" + compilerOutput);
        }
        // The above has compiled into into e.g.
        // /tmp/workspace/cache/classes/temporaryCompilationDirectory12345/io/deephaven/test/cm12862183232603186v52_0/{various
        // class files}
        // We want to atomically move it to e.g.
        // /tmp/workspace/cache/classes/io/deephaven/test/cm12862183232603186v52_0/{various class files}
        Path srcDir = Paths.get(tempDirAsString, splitPackageName);
        Path destDir = Paths.get(rootPathAsString, splitPackageName);
        try {
            Files.move(srcDir, destDir, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ioe) {
            // The move might have failed for a variety of bad reasons. However, if the reason was because
            // we lost the race to some other process, that's a harmless/desirable outcome, and we can ignore
            // it.
            if (!Files.exists(destDir)) {
                throw new UncheckedIOException("Move failed for some reason other than destination already existing",
                        ioe);
            }
        }
    }

    /**
     * Try to compile the set of files, returning a pair of success and compiler output.
     *
     * @param basePath the base path for the java classes
     * @param javaFiles the java source files
     * @return a Pair of success, and the compiler output
     */
    private Pair<Boolean, String> tryCompile(File basePath, Collection<File> javaFiles) throws IOException {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new RuntimeException("No Java compiler provided - are you using a JRE instead of a JDK?");
        }

        final File outputDirectory = Files.createTempDirectory("temporaryCompilationDirectory").toFile();

        try {
            final StringWriter compilerOutput = new StringWriter();
            final String javaClasspath = getJavaClassPath();

            final Collection<JavaFileObject> javaFileObjects = javaFiles.stream()
                    .map(f -> new JavaSourceFromFile(basePath, f)).collect(Collectors.toList());

            final boolean result = compiler.getTask(compilerOutput, null, null,
                    Arrays.asList("-d", outputDirectory.getAbsolutePath(), "-cp",
                            getClassPath() + File.pathSeparator + javaClasspath),
                    null, javaFileObjects).call();

            return new Pair<>(result, compilerOutput.toString());
        } finally {
            FileUtils.deleteRecursively(outputDirectory);
        }
    }

    /**
     * Retrieve the java class path from our existing Java class path, and IntelliJ/TeamCity environment variables.
     *
     * @return
     */
    private static String getJavaClassPath() {
        String javaClasspath;
        {
            final StringBuilder javaClasspathBuilder = new StringBuilder(System.getProperty("java.class.path"));

            final String teamCityWorkDir = System.getProperty("teamcity.build.workingDir");
            if (teamCityWorkDir != null) {
                // We are running in TeamCity, get the classpath differently
                final File[] classDirs = new File(teamCityWorkDir + "/_out_/classes").listFiles();

                for (File f : classDirs) {
                    javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                }
                final File[] testDirs = new File(teamCityWorkDir + "/_out_/test-classes").listFiles();

                for (File f : testDirs) {
                    javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
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
                                    .filter(fileName -> fileName.length() > 0)
                                    .collect(Collectors.joining(File.pathSeparator));

                            // Remove the classpath jar in question, and expand it with the files from the manifest
                            javaClasspath = Stream.of(javaClasspath.split(File.pathSeparator))
                                    .map(cp -> cp.matches(intellijClassPathJarRegex) ? filePaths : cp)
                                    .collect(Collectors.joining(File.pathSeparator));
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error extract manifest file from " + javaClasspath + ".\n", e);
            }
        }
        return javaClasspath;
    }
}
