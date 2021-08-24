/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.compilertools;

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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.security.AccessController.doPrivileged;

public class CompilerTools {
    private static final Logger log = LoggerFactory.getLogger(CompilerTools.class);
    /**
     * We pick a number just shy of 65536, leaving a little elbow room for good luck.
     */
    private static final int DEFAULT_MAX_STRING_LITERAL_LENGTH = 65500;

    private static final String JAVA_CLASS_VERSION =
        System.getProperty("java.class.version").replace('.', '_');
    private static final int MAX_CLASS_COLLISIONS = 128;

    private static final String IDENTIFYING_FIELD_NAME = "_CLASS_BODY_";

    private static final String CODEGEN_TIMEOUT_PROP = "CompilerTools.codegen.timeoutMs";
    private static final long CODEGEN_TIMEOUT_MS_DEFAULT = TimeUnit.SECONDS.toMillis(10); // 10
                                                                                          // seconds
    private static final String CODEGEN_LOOP_DELAY_PROP = "CompilerTools.codegen.retry.delay";
    private static final long CODEGEN_LOOP_DELAY_MS_DEFAULT = 100;
    private static final long codegenTimeoutMs = Configuration.getInstance()
        .getLongWithDefault(CODEGEN_TIMEOUT_PROP, CODEGEN_TIMEOUT_MS_DEFAULT);
    private static final long codegenLoopDelayMs = Configuration.getInstance()
        .getLongWithDefault(CODEGEN_LOOP_DELAY_PROP, CODEGEN_LOOP_DELAY_MS_DEFAULT);

    /**
     * Enables or disables compilation logging.
     *
     * @param logEnabled Whether or not logging should be enabled
     * @return The value of {@code logEnabled} before calling this method.
     */
    public static boolean setLogEnabled(boolean logEnabled) {
        boolean original = CompilerTools.logEnabled;
        CompilerTools.logEnabled = logEnabled;
        return original;
    }

    /*
     * NB: This is (obviously) not thread safe if code tries to write the same className to the same
     * destinationDirectory from multiple threads. Seeing as we don't currently have this use case,
     * leaving synchronization as an external concern.
     */
    public static void writeClass(final File destinationDirectory, final String className,
        final byte[] data) throws IOException {
        writeClass(destinationDirectory, className, data, null);
    }

    private static void ensureDirectories(final File file, final Supplier<String> runtimeErrMsg) {
        // File.mkdirs() checks for existrance on entry, in which case it returns false.
        // It may also return false on a failure to create.
        // Also note, two separate threads or JVMs may be running this code in parallel. It's
        // possible that we could lose the race
        // (and therefore mkdirs() would return false), but still get the directory we need (and
        // therefore exists() would return true)
        if (!file.mkdirs() && !file.isDirectory()) {
            throw new RuntimeException(runtimeErrMsg.get());
        }
    }

    /*
     * NB: This is (obviously) not thread safe if code tries to write the same className to the same
     * destinationDirectory from multiple threads. Seeing as we don't currently have this use case,
     * leaving synchronization as an external concern.
     */
    public static void writeClass(final File destinationDirectory, final String className,
        final byte[] data, final String message) throws IOException {
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
                    throw new IOException(
                        "Could not delete existing class file: " + destinationFile);
                }
            }
        }

        final File parentDir = destinationFile.getParentFile();
        ensureDirectories(parentDir,
            () -> "Unable to create missing destination directory " + parentDir.getAbsolutePath());
        if (!destinationFile.createNewFile()) {
            throw new RuntimeException(
                "Unable to create destination file " + destinationFile.getAbsolutePath());
        }
        final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream(data.length);
        byteOutStream.write(data, 0, data.length);
        final FileOutputStream fileOutStream = new FileOutputStream(destinationFile);
        byteOutStream.writeTo(fileOutStream);
        fileOutStream.close();
    }

    public static final String FORMULA_PREFIX = "io.deephaven.temp";
    public static final String DYNAMIC_GROOVY_CLASS_PREFIX = "io.deephaven.dynamic";

    public static class Context {
        private final Hashtable<String, SimplePromise<Class<?>>> knownClasses = new Hashtable<>();

        String[] dynamicPatterns = new String[] {DYNAMIC_GROOVY_CLASS_PREFIX, FORMULA_PREFIX};

        private ClassLoader getClassLoaderForFormula(final Map<String, Class<?>> parameterClasses) {
            // We should always be able to get our own class loader, even if this is invoked from
            // external code
            // that doesn't have security permissions to make ITS own class loader.
            return doPrivileged(
                (PrivilegedAction<URLClassLoader>) () -> new URLClassLoader(ucl.getURLs(), ucl) {
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

                        // Unless we are looking for a formula or Groovy class, we should use the
                        // default behavior
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
                        try {
                            // The compiler should always have access to the class-loader
                            // directories,
                            // even if code that invokes this does not.
                            return doPrivileged((PrivilegedExceptionAction<byte[]>) () -> {
                                final File destFile =
                                    new File(classDestination, name.replace('.', File.separatorChar)
                                        + JavaFileObject.Kind.CLASS.extension);
                                if (destFile.exists()) {
                                    return Files.readAllBytes(destFile.toPath());
                                }

                                for (File location : additionalClassLocations) {
                                    final File checkFile =
                                        new File(location, name.replace('.', File.separatorChar)
                                            + JavaFileObject.Kind.CLASS.extension);
                                    if (checkFile.exists()) {
                                        return Files.readAllBytes(checkFile.toPath());
                                    }
                                }

                                throw new FileNotFoundException(name);
                            });
                        } catch (final PrivilegedActionException pae) {
                            final Exception inner = pae.getException();
                            if (inner instanceof IOException) {
                                throw (IOException) inner;
                            } else {
                                throw new RuntimeException(inner);
                            }
                        }
                    }
                });
        }

        private static class WritableURLClassLoader extends URLClassLoader {
            private WritableURLClassLoader(URL[] urls, ClassLoader parent) {
                super(urls, parent);
            }

            @Override
            protected synchronized Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
                Class<?> clazz = findLoadedClass(name);
                if (clazz != null) {
                    return clazz;
                }

                try {
                    clazz = findClass(name);
                } catch (ClassNotFoundException e) {
                    clazz = getParent().loadClass(name);
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

        private final File classDestination;
        private final Set<File> additionalClassLocations;
        private volatile WritableURLClassLoader ucl;

        public Context(File classDestination) {
            this(classDestination, Context.class.getClassLoader());
        }

        public Context(File classDestination, ClassLoader parentClassLoader) {
            this.classDestination = classDestination;
            ensureDirectories(this.classDestination,
                () -> "Failed to create missing class destination directory " +
                    classDestination.getAbsolutePath());
            additionalClassLocations = new LinkedHashSet<>();

            URL[] urls = new URL[1];
            try {
                urls[0] = (classDestination.toURI().toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException("", e);
            }
            // We should be able to create this class loader, even if this is invoked from external
            // code
            // that does not have sufficient security permissions.
            this.ucl = doPrivileged(
                (PrivilegedAction<WritableURLClassLoader>) () -> new WritableURLClassLoader(urls,
                    parentClassLoader));
        }

        protected void addClassSource(File classSourceDirectory) {
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

        public File getFakeClassDestination() {
            // Groovy classes need to be written out to a location where they can be found by the
            // compiler
            // (so that filters and formulae can use them).
            //
            // We don't want the regular runtime class loader to find them, because then they get
            // "stuck" in there
            // even if the class itself changes, and we can't forget it. So instead we use a
            // single-use class loader
            // for each formula, that will always read the class from disk.
            return null;
        }

        private File getClassDestination() {
            return classDestination;
        }

        public String getClassPath() {
            StringBuilder sb = new StringBuilder();
            sb.append(classDestination.getAbsolutePath());
            synchronized (additionalClassLocations) {
                for (File classLoc : additionalClassLocations) {
                    sb.append(File.pathSeparatorChar).append(classLoc.getAbsolutePath());
                }
            }
            return sb.toString();
        }

        public WritableURLClassLoader getClassLoader() {
            return ucl;
        }

        public void setParentClassLoader(final ClassLoader parentClassLoader) {
            // The system should always be able to create this class loader, even if invoked from
            // something that
            // doesn't have the right security permissions for it.
            ucl = doPrivileged(
                (PrivilegedAction<WritableURLClassLoader>) () -> new WritableURLClassLoader(
                    ucl.getURLs(), parentClassLoader));
        }

        public void cleanup() {
            FileUtils.deleteRecursively(classDestination);
        }
    }

    private static volatile Context defaultContext = null;

    private static Context getDefaultContext() {
        if (defaultContext == null) {
            synchronized (CompilerTools.class) {
                if (defaultContext == null) {
                    defaultContext =
                        new Context(new File(Configuration.getInstance().getWorkspacePath() +
                            File.separator + "cache" + File.separator + "classes"));
                }
            }
        }
        return defaultContext;
    }

    /**
     * Sets the default context.
     *
     * @param context the script session's compiler context
     * @throws IllegalStateException if default context is already set
     * @throws NullPointerException if context is null
     */
    public static synchronized void setDefaultContext(final Context context) {
        if (defaultContext != null) {
            throw new IllegalStateException(
                "It's too late to set default context; it's already set to: " + defaultContext);
        }
        defaultContext = Objects.requireNonNull(context);
    }

    private static final ThreadLocal<Context> currContext =
        ThreadLocal.withInitial(CompilerTools::getDefaultContext);

    public static void resetContext() {
        setContext(new Context(new File(Configuration.getInstance().getWorkspacePath()
            + File.separator + "cache" + File.separator + "classes")));
    }

    public static void setContext(@Nullable Context context) {
        if (context == null) {
            currContext.remove();
        } else {
            currContext.set(context);
        }
    }

    public static Context getContext() {
        return currContext.get();
    }

    public static <RETURN_TYPE> RETURN_TYPE doWithContext(@NotNull final Context context,
        @NotNull final Supplier<RETURN_TYPE> action) {
        final Context originalContext = getContext();
        try {
            setContext(context);
            return action.get();
        } finally {
            setContext(originalContext);
        }
    }

    private static boolean logEnabled =
        Configuration.getInstance().getBoolean("CompilerTools.logEnabledDefault");

    public static Class<?> compile(String className, String classBody, String packageNameRoot) {
        return compile(className, classBody, packageNameRoot, null, Collections.emptyMap());
    }

    public static Class<?> compile(String className, String classBody, String packageNameRoot,
        Map<String, Class<?>> parameterClasses) {
        return compile(className, classBody, packageNameRoot, null, parameterClasses);
    }

    public static Class<?> compile(String className, String classBody, String packageNameRoot,
        StringBuilder codeLog) {
        return compile(className, classBody, packageNameRoot, codeLog, Collections.emptyMap());
    }

    /**
     * Compile a class.
     *
     * @param className Class name
     * @param classBody Class body, before update with "$CLASS_NAME$" replacement and package name
     *        prefixing
     * @param packageNameRoot Package name prefix
     * @param codeLog Optional "log" for final class code
     * @param parameterClasses Generic parameters, empty if none required
     * @return The compiled class
     */
    public static Class<?> compile(@NotNull final String className,
        @NotNull final String classBody,
        @NotNull final String packageNameRoot,
        @Nullable final StringBuilder codeLog,
        @NotNull final Map<String, Class<?>> parameterClasses) {
        SimplePromise<Class<?>> promise;
        final boolean promiseAlreadyMade;

        final Context context = getContext();

        synchronized (context) {
            promise = context.knownClasses.get(classBody);
            if (promise != null) {
                promiseAlreadyMade = true;
            } else {
                promise = new SimplePromise<>();
                context.knownClasses.put(classBody, promise);
                promiseAlreadyMade = false;
            }
        }

        // Someone else has already made the promise. I'll just wait for the answer.
        if (promiseAlreadyMade) {
            return promise.getResult();
        }

        // It's my job to fulfill the promise
        try {
            return compileHelper(className, classBody, packageNameRoot, codeLog, parameterClasses,
                context);
        } catch (RuntimeException e) {
            promise.setException(e);
            throw e;
        }
    }

    private static Class<?> compileHelper(@NotNull final String className,
        @NotNull final String classBody,
        @NotNull final String packageNameRoot,
        @Nullable final StringBuilder codeLog,
        @NotNull final Map<String, Class<?>> parameterClasses,
        @NotNull final Context context) {
        // NB: We include class name hash in order to (hopefully) account for case insensitive file
        // systems.
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
                // B. Lose a race to another process on the same file system which is compiling the
                // identical formula
                // C. Lose a race to another process on the same file system compiling a different
                // formula that
                // happens to have the same hash (same packageName).
                // However, regardless of A-C, there will be *some* class being found (i.e.
                // tryLoadClassByFqName won't
                // return null).
                maybeCreateClass(className, classBody, packageName, fqClassName);

                // We could be running on a screwy filesystem that is slow (e.g. NFS).
                // If we wrote a file and can't load it ... then give the filesystem some time.
                result = tryLoadClassByFqName(fqClassName, parameterClasses);
                try {
                    final long deadline =
                        System.currentTimeMillis() + codegenTimeoutMs - codegenLoopDelayMs;
                    while (result == null && System.currentTimeMillis() < deadline) {
                        Thread.sleep(codegenLoopDelayMs);
                        result = tryLoadClassByFqName(fqClassName, parameterClasses);
                    }
                } catch (InterruptedException ignored) {
                    // we got interrupted, just quit looping and ignore it.
                }

                if (result == null) {
                    throw new IllegalStateException(
                        "Should have been able to load *some* class here");
                }
            }

            final String identifyingFieldValue = loadIdentifyingField(result);

            // We have a class. It either contains the formula we are looking for (cases 2, A, and
            // B) or a different
            // formula with the same name (cases 3 and C). In either case, we should store the
            // result in our cache,
            // either fulfilling an existing promise or making a new, fulfilled promise.
            synchronized (context) {
                // Note we are doing something kind of subtle here. We are removing an entry whose
                // key was matched by
                // value equality and replacing it with a value-equal but reference-different string
                // that is a static
                // member of the class we just loaded. This should be easier on the garbage
                // collector because we are
                // replacing a calculated value with a classloaded value and so in effect we are
                // "canonicalizing" the
                // string. This is important because these long strings stay in knownClasses
                // forever.
                SimplePromise<Class<?>> p = context.knownClasses.remove(identifyingFieldValue);
                if (p == null) {
                    // If we encountered a different class than the one we're looking for, make a
                    // fresh promise and
                    // immediately fulfill it. This is for the purpose of populating the cache in
                    // case someone comes
                    // looking for that class later. Rationale: we already did all the classloading
                    // work; no point in
                    // throwing it away now, even though this is not the class we're looking for.
                    p = new SimplePromise<>();
                }
                context.knownClasses.put(identifyingFieldValue, p);
                // It's also possible that some other code has already fulfilled this promise with
                // exactly the same
                // class. That's ok though: the promise code does not reject multiple sets to the
                // identical value.
                p.setResultFriendly(result);
            }

            // If the class we found was indeed the class we were looking for, then return it
            if (classBody.equals(identifyingFieldValue)) {
                // Cases 2, A, and B.
                if (codeLog != null) {
                    // If the caller wants a textual copy of the code we either made, or just found
                    // in the cache.
                    codeLog.append(makeFinalCode(className, classBody, packageName));
                }
                return result;
            }
            // Try the next hash name
        }
        throw new IllegalStateException(
            "Found too many collisions for package name root " + packageNameRoot
                + ", class name=" + className
                + ", class body hash=" + classBodyHash + " - contact Deephaven support!");
    }

    private static Class<?> tryLoadClassByFqName(String fqClassName,
        Map<String, Class<?>> parameterClasses) {
        try {
            return getContext().getClassLoaderForFormula(parameterClasses).loadClass(fqClassName);
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
        classBody += "    public static String " + IDENTIFYING_FIELD_NAME + " = "
            + joinedEscapedBody + ";\n}";
        return "package " + packageName + ";\n" + classBody;
    }

    /**
     * Transform a string into the corresponding Java source code that compiles into that string.
     * This involves escaping special characters, surrounding it with quotes, and (if the string is
     * larger than the max string length for Java literals), splitting it into substrings and
     * constructing a call to String.join() that combines those substrings.
     */
    public static String createEscapedJoinedString(final String originalString) {
        return createEscapedJoinedString(originalString, DEFAULT_MAX_STRING_LITERAL_LENGTH);
    }

    public static String createEscapedJoinedString(final String originalString,
        int maxStringLength) {
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
        // Number of bytes in the "modified UTF-8" representation of the substring we are currently
        // scanning.
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
        // 2. originalString was empty and so splits is empty and we need to add a single empty
        // string to splits
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

    static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension),
                Kind.SOURCE);
            this.code = code;
        }

        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    static class JavaSourceFromFile extends SimpleJavaFileObject {
        private static final int JAVA_LENGTH = Kind.SOURCE.extension.length();
        final String code;

        JavaSourceFromFile(File basePath, File file) {
            super(URI.create("string:///" + createName(basePath, file).replace('.', '/')
                + Kind.SOURCE.extension), Kind.SOURCE);
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

        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private static void maybeCreateClass(String className, String code, String packageName,
        String fqClassName) {
        final String finalCode = makeFinalCode(className, code, packageName);

        // The 'compile' action does a bunch of things that need security permissions; this always
        // needs to run
        // with elevated permissions.
        if (logEnabled) {
            log.info().append("Generating code ").append(finalCode).endl();
        }

        final Context ctx = getContext();
        final File ctxClassDestination = ctx.getClassDestination();

        final String[] splitPackageName = packageName.split("\\.");
        if (splitPackageName.length == 0) {
            throw new RuntimeException(
                String.format("packageName %s expected to have at least one .", packageName));
        }
        final String[] truncatedSplitPackageName =
            Arrays.copyOf(splitPackageName, splitPackageName.length - 1);

        // Get the destination root directory (e.g. /tmp/workspace/cache/classes) and populate it
        // with the package
        // directories (e.g. io/deephaven/test) if they are not already there. This will be useful
        // later.
        // Also create a temp directory e.g.
        // /tmp/workspace/cache/classes/temporaryCompilationDirectory12345
        // This temp directory will be where the compiler drops files into, e.g.
        // /tmp/workspace/cache/classes/temporaryCompilationDirectory12345/io/deephaven/test/cm12862183232603186v52_0/Formula.class
        // Foreshadowing: we will eventually atomically move cm12862183232603186v52_0 from the above
        // to
        // /tmp/workspace/cache/classes/io/deephaven/test
        // Note: for this atomic move to work, this temp directory must be on the same file system
        // as the destination
        // directory.
        final String rootPathAsString;
        final String tempDirAsString;
        try {
            final Pair<String, String> resultPair = AccessController
                .doPrivileged((PrivilegedExceptionAction<Pair<String, String>>) () -> {
                    final String rootPathString = ctxClassDestination.getAbsolutePath();
                    final Path rootPathWithPackage =
                        Paths.get(rootPathString, truncatedSplitPackageName);
                    final File rpf = rootPathWithPackage.toFile();
                    ensureDirectories(rpf,
                        () -> "Couldn't create package directories: " + rootPathWithPackage);
                    final Path tempPath = Files.createTempDirectory(Paths.get(rootPathString),
                        "temporaryCompilationDirectory");
                    final String tempPathString = tempPath.toFile().getAbsolutePath();
                    return new Pair<>(rootPathString, tempPathString);
                });
            rootPathAsString = resultPair.first;
            tempDirAsString = resultPair.second;
        } catch (PrivilegedActionException pae) {
            throw new RuntimeException(pae.getException());
        }

        try {
            maybeCreateClassHelper(fqClassName, finalCode, splitPackageName, ctx, rootPathAsString,
                tempDirAsString);
        } finally {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    FileUtils.deleteRecursively(new File(tempDirAsString));
                } catch (Exception e) {
                    // ignore errors here
                }
                return null;
            });
        }
    }

    private static void maybeCreateClassHelper(String fqClassName, String finalCode,
        String[] splitPackageName,
        Context ctx, String rootPathAsString, String tempDirAsString) {
        final StringWriter compilerOutput = new StringWriter();

        final JavaCompiler compiler = AccessController
            .doPrivileged((PrivilegedAction<JavaCompiler>) ToolProvider::getSystemJavaCompiler);
        if (compiler == null) {
            throw new RuntimeException(
                "No Java compiler provided - are you using a JRE instead of a JDK?");
        }

        final String classPathAsString =
            ctx.getClassPath() + File.pathSeparator + getJavaClassPath();
        final List<String> compilerOptions =
            AccessController.doPrivileged((PrivilegedAction<List<String>>) () -> Arrays.asList("-d",
                tempDirAsString, "-cp", classPathAsString));

        final StandardJavaFileManager fileManager =
            compiler.getStandardFileManager(null, null, null);

        final boolean result = compiler.getTask(compilerOutput,
            fileManager,
            null,
            compilerOptions,
            null,
            Collections.singletonList(new JavaSourceFromString(fqClassName, finalCode)))
            .call();
        if (!result) {
            throw new RuntimeException(
                "Error compiling class " + fqClassName + ":\n" + compilerOutput.toString());
        }
        // The above has compiled into into e.g.
        // /tmp/workspace/cache/classes/temporaryCompilationDirectory12345/io/deephaven/test/cm12862183232603186v52_0/{various
        // class files}
        // We want to atomically move it to e.g.
        // /tmp/workspace/cache/classes/io/deephaven/test/cm12862183232603186v52_0/{various class
        // files}
        // Our strategy
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                Path srcDir = Paths.get(tempDirAsString, splitPackageName);
                Path destDir = Paths.get(rootPathAsString, splitPackageName);
                try {
                    Files.move(srcDir, destDir, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException ioe) {
                    // The move might have failed for a variety of bad reasons. However, if the
                    // reason was because
                    // we lost the race to some other process, that's a harmless/desirable outcome,
                    // and we can ignore
                    // it.
                    if (!Files.exists(destDir)) {
                        throw new IOException(
                            "Move failed for some reason other than destination already existing",
                            ioe);
                    }
                }
                return null;
            });
        } catch (PrivilegedActionException pae) {
            throw new RuntimeException(pae.getException());
        }
    }

    /**
     * Try to compile the set of files, returning a pair of success and compiler output.
     *
     * @param basePath the base path for the java classes
     * @param javaFiles the java source files
     * @return a Pair of success, and the compiler output
     */
    public static Pair<Boolean, String> tryCompile(File basePath, Collection<File> javaFiles)
        throws IOException {
        try {
            // We need multiple filesystem accesses et al, so make this whole section privileged.
            return AccessController
                .doPrivileged((PrivilegedExceptionAction<Pair<Boolean, String>>) () -> {

                    final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
                    if (compiler == null) {
                        throw new RuntimeException(
                            "No Java compiler provided - are you using a JRE instead of a JDK?");
                    }

                    final File outputDirectory =
                        Files.createTempDirectory("temporaryCompilationDirectory").toFile();

                    try {
                        final StringWriter compilerOutput = new StringWriter();
                        final Context ctx = getContext();
                        final String javaClasspath = getJavaClassPath();

                        final Collection<JavaFileObject> javaFileObjects =
                            javaFiles.stream().map(f -> new JavaSourceFromFile(basePath, f))
                                .collect(Collectors.toList());

                        final boolean result = compiler.getTask(compilerOutput, null, null,
                            Arrays.asList("-d", outputDirectory.getAbsolutePath(), "-cp",
                                ctx.getClassPath() + File.pathSeparator + javaClasspath),
                            null, javaFileObjects).call();

                        return new Pair<>(result, compilerOutput.toString());

                    } finally {
                        FileUtils.deleteRecursively(outputDirectory);
                    }
                });
        } catch (final PrivilegedActionException pae) {
            if (pae.getException() instanceof IOException) {
                throw (IOException) pae.getException();
            } else {
                throw new RuntimeException(pae.getException());
            }
        }
    }

    /**
     * Retrieve the java class path from our existing Java class path, and IntelliJ/TeamCity
     * environment variables.
     * 
     * @return
     */
    public static String getJavaClassPath() {
        String javaClasspath;
        {
            final StringBuilder javaClasspathBuilder =
                new StringBuilder(System.getProperty("java.class.path"));

            final String teamCityWorkDir = System.getProperty("teamcity.build.workingDir");
            if (teamCityWorkDir != null) {
                // We are running in TeamCity, get the classpath differently
                final File classDirs[] = new File(teamCityWorkDir + "/_out_/classes").listFiles();

                for (File f : classDirs) {
                    javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                }
                final File testDirs[] =
                    new File(teamCityWorkDir + "/_out_/test-classes").listFiles();

                for (File f : testDirs) {
                    javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                }

                final File jars[] = FileUtils.findAllFiles(new File(teamCityWorkDir + "/lib"));
                for (File f : jars) {
                    if (f.getName().endsWith(".jar")) {
                        javaClasspathBuilder.append(File.pathSeparator).append(f.getAbsolutePath());
                    }
                }
            }
            javaClasspath = javaClasspathBuilder.toString();
        }

        // IntelliJ will bundle a very large class path into an empty jar with a Manifest that will
        // define the full class path
        // Look for this being used during compile time, so the full class path can be sent into the
        // compile call
        final String intellijClassPathJarRegex = ".*classpath[0-9]*\\.jar.*";
        if (javaClasspath.matches(intellijClassPathJarRegex)) {
            try {
                final Enumeration<URL> resources =
                    CompilerTools.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
                final Attributes.Name createdByAttribute = new Attributes.Name("Created-By");
                final Attributes.Name classPathAttribute = new Attributes.Name("Class-Path");
                while (resources.hasMoreElements()) {
                    // Check all manifests -- looking for the Intellij created one
                    final Manifest manifest = new Manifest(resources.nextElement().openStream());
                    final Attributes attributes = manifest.getMainAttributes();
                    final Object createdBy = attributes.get(createdByAttribute);
                    if ("IntelliJ IDEA".equals(createdBy)) {
                        final String extendedClassPath =
                            (String) attributes.get(classPathAttribute);
                        if (extendedClassPath != null) {
                            // Parses the files in the manifest description an changes their format
                            // to drop the "file:/" and
                            // use the default path separator
                            final String filePaths = Stream.of(extendedClassPath.split("file:/"))
                                .map(String::trim)
                                .filter(fileName -> fileName.length() > 0)
                                .collect(Collectors.joining(File.pathSeparator));

                            // Remove the classpath jar in question, and expand it with the files
                            // from the manifest
                            javaClasspath = Stream.of(javaClasspath.split(File.pathSeparator))
                                .map(cp -> cp.matches(intellijClassPathJarRegex) ? filePaths : cp)
                                .collect(Collectors.joining(File.pathSeparator));
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(
                    "Error extract manifest file from " + javaClasspath + ".\n", e);
            }
        }
        return javaClasspath;
    }

    private static class SimplePromise<R> {
        private R result;
        private RuntimeException exception;

        public synchronized R getResult() {
            while (true) {
                if (result != null) {
                    return result;
                }
                if (exception != null) {
                    throw exception;
                }
                try {
                    wait();
                } catch (InterruptedException ie) {
                    throw new IllegalStateException("Interrupted while waiting", ie);
                }
            }
        }

        public synchronized void setResultFriendly(R newResult) {
            if (newResult == null) {
                throw new IllegalStateException("null result not allowed");
            }
            // We are "friendly" in the sense that we don't reject multiple sets to the same value.
            if (result == newResult) {
                return;
            }
            checkState();
            result = newResult;
            notifyAll();
        }

        public synchronized void setException(RuntimeException newException) {
            if (newException == null) {
                throw new IllegalStateException("null exception not allowed");
            }
            checkState();
            exception = newException;
            notifyAll();
        }

        private void checkState() {
            if (result != null || exception != null) {
                throw new IllegalStateException("State is already set");
            }
        }
    }

    public static void main(String[] args) {
        final String sillyFuncFormat = String.join("\n",
            "    public int sillyFunc%d() {",
            "        final String temp0 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp1 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp2 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp3 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp4 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp5 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp6 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp7 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp8 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        final String temp9 = \"ᵈᵉᵉᵖʰᵃᵛᵉⁿ__½¼⅒___\uD83D\uDC96___0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\";",
            "        return temp0.length() + temp1.length() + temp2.length() + temp3.length() + temp4.length() + temp5.length() + temp6.length() + temp7.length() + temp8.length() + temp9.length();",
            "    }");

        final StringBuilder sb = new StringBuilder();
        sb.append(String.join("\n",
            "public class $CLASSNAME$ implements java.util.function.Function<Integer, Integer> {",
            "    private final int n;",
            "    public $CLASSNAME$(int n) {",
            "        this.n = n;",
            "    }",
            "    public Integer apply(Integer v) {",
            "        return n + v;",
            "    }"));
        for (int ii = 0; ii < 100; ++ii) {
            sb.append("\n");
            sb.append(String.format(sillyFuncFormat, ii));
        }
        sb.append("\n}");
        final String programText = sb.toString();
        System.out.println(programText);

        StringBuilder codeLog = new StringBuilder();
        try {
            final Class<?> clazz =
                compile("Test", programText, "com.deephaven.test", codeLog, Collections.emptyMap());
            final Constructor<?> constructor = clazz.getConstructor(int.class);
            Function<Integer, Integer> obj =
                (Function<Integer, Integer>) constructor.newInstance(17);
            final int result = obj.apply(5);
            if (result != 22) {
                throw new Exception(String.format("Expected 22, got %d", result));
            }
        } catch (Exception e) {
            System.out.printf("sad: %s%n", e);
        }
    }
}
