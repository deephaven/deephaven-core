/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.base.Function;
import io.deephaven.base.Procedure;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.logger.Logger;
import org.jdom2.Attribute;
import org.jdom2.Element;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Landing place for general purpose utility functions.
 */
public class Utils {
    public static final String MAC_OPTION_STRING = "\u2325";
    public static final String MAC_COMMAND_STRING = "\u2318";

    /**
     * Convert IOException to UncheckedIOException.
     *
     * @param r the stuff to run
     */
    public static void unCheck(final Procedure.ThrowingNullary<IOException> r) {
        try {
            r.call();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Convert IOException to UncheckedIOException.
     *
     * @param r the stuff to run
     * @return the result of the stuff
     */
    public static <T> T unCheck(final Function.ThrowingNullary<T, IOException> r) {
        try {
            return r.call();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * syntactic sugar to use empty strings instead of null.
     *
     * @param s the string to check
     * @return the original string, or "" if it was null
     */
    public static String unNull(String s) {
        return s != null ? s : "";
    }

    /**
     * Run something until it is successfully finished without an interrupted exception.
     *
     * @param thing the thing to run despite interruptions.
     * @param name what to call the thing - for logging
     */
    public static void runWithoutInterruption(Logger log,
        Procedure.ThrowingNullary<InterruptedException> thing, String name) {
        do {
            try {
                thing.call();
                return;
            } catch (InterruptedException ignore) {
                log.warn().append("Caught InterruptedException while running ").append(name).endl();
            }
        } while (true);
    }

    /**
     * Sleep, ignoring any interruptions
     *
     * @param millisToSleep millis to sleep
     * @return the number of millis slept
     */
    public static long sleepIgnoringInterruptions(final long millisToSleep) {
        long currentTime = System.currentTimeMillis();
        long startTime = currentTime;
        final long endTime = currentTime + millisToSleep;
        while (currentTime < endTime) {
            try {
                Thread.sleep(endTime - currentTime);
            } catch (InterruptedException ignored) {
            }
            currentTime = System.currentTimeMillis();
        }
        return currentTime - startTime;
    }

    /**
     * Checks if an {@link Element} is empty, ignoring the specified set of attributes. An empty
     * element contains no content and no attributes aside from those indicated in ignoredAttrs
     *
     * @param elem The element to check
     * @param ignoredAttrs A set of attributes that can be present while this element is still
     *        considered empty.
     *
     * @return true if the element contained no content or attributes excluding those indicated in
     *         ignoredAttrs
     */
    public static boolean isEmptyElement(Element elem, String... ignoredAttrs) {
        final List<Attribute> attrs = elem.getAttributes();
        if (!attrs.isEmpty() && ignoredAttrs.length != 0) {
            final List<String> ignored = Arrays.asList(ignoredAttrs);
            if (attrs.stream().anyMatch(attr -> !ignored.contains(attr.getName()))) {
                return false;
            }
        }

        return elem.getChildren().isEmpty();
    }

    /**
     * Wrap the specified element in a new one with the specified name
     * 
     * @param wrapperName The name of the wrapper element to create
     * @param wrapee The element being wrapped.
     *
     * @return A new element with the specified name and element as it's content
     */
    public static Element wrapElement(@NotNull String wrapperName, @NotNull Element wrapee) {
        return new Element(wrapperName).addContent(wrapee);
    }

    /**
     * Get the single element that was wrapped by a previous call to
     * {@link #wrapElement(String, Element)}
     *
     * @param wrapperName The name of the wrapper
     * @param parentElem The element containing the wrapper
     *
     * @return The element that was rapped or null, if the wrapper was not found.
     */
    public static Element unwrapElement(@NotNull String wrapperName, @NotNull Element parentElem) {
        final Element wrapper = parentElem.getChild(wrapperName);
        if (wrapper == null) {
            return null;
        }

        return wrapper.getChildren().isEmpty() ? null : wrapper.getChildren().get(0);
    }

    public static LocalDateTime getLastModifiedTime(File f) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(f.lastModified()),
            ZoneId.systemDefault());
    }

    /**
     * Get a {@code Comparator<String>} that treats its inputs as file names in the same directory
     * basePath, and compares each file by its modified time
     *
     * @param dir The root path in which both files reside.
     * @return A new {@link Comparator<String>}.
     */
    public static Comparator<String> getModifiedTimeComparator(File dir, boolean descending) {
        final int direction = descending ? -1 : 1;
        return (f1, f2) -> {
            final LocalDateTime f1Time = Utils.getLastModifiedTime(new File(dir, f1));
            final LocalDateTime f2Time = Utils.getLastModifiedTime(new File(dir, f2));

            if (f1Time.isBefore(f2Time)) {
                return -direction;
            } else if (f1Time.isAfter(f2Time)) {
                return direction;
            }

            return 0;
        };
    }

    /**
     * Get a {@link Comparator<File>} that treats its inputs as file names in the same directory
     * basePath, and compares each file by its modified time
     *
     * @return A new comparator.
     */
    public static Comparator<File> getModifiedTimeComparator(boolean descending) {
        final int direction = descending ? -1 : 1;
        return (f1, f2) -> {
            final LocalDateTime f1Time = Utils.getLastModifiedTime(f1);
            final LocalDateTime f2Time = Utils.getLastModifiedTime(f2);

            if (f1Time.isBefore(f2Time)) {
                return -direction;
            } else if (f1Time.isAfter(f2Time)) {
                return direction;
            }

            return 0;
        };
    }

    public static RuntimeException sneakyThrow(Throwable t) {
        if (t == null) {
            throw new NullPointerException();
        }
        dirtyTrick(t);
        assert false : "Can't get here";
        return new RuntimeException(t); // can't get here
    }

    private static <T extends Throwable> void dirtyTrick(Throwable t) throws T {
        throw (T) t;
    }

    public static URLClassLoader cleanRoom() {
        Set<URL> all = new LinkedHashSet<>();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        while (cl != null) {
            if (cl instanceof URLClassLoader) {
                for (URL url : ((URLClassLoader) cl).getURLs()) {
                    all.add(url);
                }
            }
            cl = cl.getParent();
        }
        // We should be able to create this class loader even if this is invoked from external code
        // that lacks that permission.
        return AccessController.doPrivileged(
            (PrivilegedAction<URLClassLoader>) () -> new URLClassLoader(all.toArray(new URL[0]),
                null));
    }

    /**
     * Close an {@link AutoCloseable} object and discard any exceptions. <br>
     * NB: Per {@link AutoCloseable#close()}, <i>Note that unlike the close method of
     * java.io.Closeable, this close method is not required to be idempotent. In other words,
     * <b>calling this close method more than once may have some visible side effect,</b> unlike
     * Closeable.close which is required to have no effect if called more than once. However,
     * implementers of this interface are strongly encouraged to make their close methods
     * idempotent.</i>
     * 
     * @param autoCloseable The resource to close.
     */
    public static void ensureClosed(@NotNull final AutoCloseable autoCloseable) {
        try {
            autoCloseable.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Reverse a subset of an array.
     *
     * @param array The array in question
     * @param start The starting index to reverse (inclusive)
     * @param end The ending index to reverse (inclusive)
     */
    public static <T> void reverseArraySubset(T[] array, int start, int end) {
        if (array == null) {
            return;
        }

        if (start < 0 || end > array.length - 1) {
            throw new IllegalArgumentException("Invalid indices to reverse array: (" + start + ", "
                + end + ") allowed " + "(0, " + (array.length - 1) + ")");
        }

        int i = start;
        int j = end;
        T tmp;

        while (j > i) {
            tmp = array[j];
            array[j] = array[i];
            array[i] = tmp;
            j--;
            i++;
        }
    }

    /**
     * Does a line by line comparison of two files to check if they are the same. Can skip the first
     * N lines.
     *
     * @param fileA the first file
     * @param fileB the second file
     * @param skipLines how many lines to skip before comparing
     * @return true if contents are equal, false otherwise
     */
    public static boolean areFileLinesEqual(Path fileA, Path fileB, int skipLines) {
        try (BufferedReader readerA = Files.newBufferedReader(fileA);
            BufferedReader readerB = Files.newBufferedReader(fileB)) {

            String lineA = readerA.readLine();
            String lineB = readerB.readLine();

            int lineNumber = 0;
            while (lineA != null && lineB != null) {
                if (lineNumber >= skipLines && !lineA.equals(lineB)) {
                    return false;
                }

                lineA = readerA.readLine();
                lineB = readerB.readLine();
                lineNumber++;
            }

            // Check that both files terminated at the same time
            return lineA == null && lineB == null;

        } catch (IOException ioe) {
            // If an IOException occurs, assume these files are different
            return false;
        }
    }

    /**
     * Changes if a file extension on a Path. Assumes the current extension starts with a dot.
     *
     * @param path the path to the file to change
     * @param extension the extension
     * @return a Path to a file with the new extension
     */
    public static Path changeFileExtension(Path path, String extension) {
        String fileName = path.getFileName().toString();
        if (fileName.endsWith(extension)) {
            return path;
        } else {
            int index = fileName.lastIndexOf('.');
            if (index > 0) {
                fileName = fileName.substring(0, index);
            }
            fileName = fileName + extension;
            return path.getParent().resolve(fileName);
        }
    }

    /**
     * Anonymous inner classes return "" as simple name. In most cases, we want the SimpleName to
     * reflect the class being overridden.
     * <p>
     * For example, <code>
     *     x = new SomeClass() { @Override ... };
     *     String name = getSimpleNameFor(x); // returns "SomeClass"
     * </code>
     * </p>
     *
     * @param o the object used to get the class for which to return the SimpleName
     * @return The SimpleName of the object's class, or of its superclass
     */
    @SuppressWarnings("WeakerAccess")
    public static String getSimpleNameFor(@NotNull Object o) {
        return getSimpleNameFor(o.getClass());
    }

    /**
     * Anonymous inner classes return "" as simple name. In most cases, we want the SimpleName to
     * reflect the class being overridden.
     *
     * @param objectClass the class for which to return the SimpleName
     * @return The SimpleName of the class, or of its superclass
     */
    @SuppressWarnings("WeakerAccess")
    public static String getSimpleNameFor(@NotNull Class<?> objectClass) {
        String simpleName = objectClass.getSimpleName();
        // noinspection ConstantConditions // objectClass could hypothetically be null as result of
        // getSuperClass
        while (simpleName.isEmpty() && objectClass != null) {
            objectClass = objectClass.getSuperclass();
            simpleName = objectClass.getSimpleName();
        }
        return simpleName;
    }

    /**
     * require (o instanceof type)
     */
    public static <T> T castTo(Object o, String name, Class<T> type, int numCallsBelowRequirer) {
        io.deephaven.base.verify.Require.instanceOf(o, name, type, numCallsBelowRequirer);
        // noinspection unchecked
        return (T) o;
    }

    /**
     * require (o instanceof type)
     */
    public static <T> T castTo(Object o, String name, Class<T> type) {
        return castTo(o, name, type, 1);
    }

    /**
     * Describe the object in a standardized format without accessing its fields or otherwise
     * risking interacting with partially-initialized state.
     *
     * @param object The object
     * @return The description
     */
    public static String makeReferentDescription(@NotNull final Object object) {
        return getSimpleNameFor(object) + '-' + System.identityHashCode(object);
    }

    /**
     * Append the non-null argument in the same format as {@link #makeReferentDescription(Object)}.
     */
    public static LogOutput.ObjFormatter<Object> REFERENT_FORMATTER =
        (logOutput, object) -> logOutput.append(getSimpleNameFor(object)).append('-')
            .append(System.identityHashCode(object));

    /**
     * Get the major Java version (e.g. 8, 11). Throw an exception if it can't be determined, or if
     * it isn't a Deephaven-supported version. Currently supported versions include:
     * <ul>
     * <li>1.8 (returned as 8)</li>
     * <li>11</li>
     * </ul>
     *
     * @return the major Java version
     */
    public static int getMajorJavaVersion() {
        final String versionString = System.getProperty("java.version");
        if (versionString == null) {
            throw new RuntimeException("Unable to determine Java version");
        }

        if (versionString.startsWith("1.8")) {
            return 8;
        }

        if (versionString.startsWith("11")) {
            return 11;
        }

        if (versionString.startsWith("13")) {
            return 13;
        }

        throw new RuntimeException("Unsupported Java version: " + versionString);
    }


    // region privileged actions

    /**
     * Perform a file existence check in a privileged context.
     *
     * @param file The File to check
     * @return same as File.exists
     */
    public static boolean fileExistsPrivileged(final File file) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) file::exists);
    }

    /**
     * Perform an IsDirectory check in a privileged context.
     *
     * @param file the File to check
     * @return same as File.isDirectory
     */
    public static boolean fileIsDirectoryPrivileged(final File file) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) file::isDirectory);
    }

    /**
     * Perform an IsDirectory check in a privileged context.
     *
     * @param path the File to check
     * @return same as File.isDirectory
     */
    public static boolean fileIsDirectoryPrivileged(final Path path, final LinkOption... options) {
        return AccessController
            .doPrivileged((PrivilegedAction<Boolean>) () -> Files.isDirectory(path, options));
    }

    public static DirectoryStream<Path> fileGetDirectoryStream(Path dir,
        DirectoryStream.Filter<? super Path> filter) throws IOException {
        try {
            return AccessController
                .doPrivileged((PrivilegedExceptionAction<DirectoryStream<Path>>) () -> Files
                    .newDirectoryStream(dir, filter));
        } catch (final PrivilegedActionException pae) {
            if (pae.getException() instanceof IOException) {
                throw (IOException) pae.getException();
            } else {
                throw new RuntimeException(pae.getException());
            }
        }
    }


    /**
     * Get an absolute File in a privileged context.
     *
     * @param file the File to check
     * @return same as File.getAbsoluteFile
     */
    public static File fileGetAbsoluteFilePrivileged(final File file) {
        return AccessController.doPrivileged((PrivilegedAction<File>) file::getAbsoluteFile);
    }

    /**
     * Get an absolute path in a privileged context.
     *
     * @param file the File to check
     * @return same as File.getAbsolutePath
     */
    public static String fileGetAbsolutePathPrivileged(final File file) {
        return AccessController.doPrivileged((PrivilegedAction<String>) file::getAbsolutePath);
    }

    /**
     * Create the directory specified by this File, excluding parent directories, in a privileged
     * context.
     *
     * @param file The directory to create
     * @return same as File.mkdir
     */
    @SuppressWarnings("UnusedReturnValue")
    public static boolean fileMkdirPrivileged(final File file) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) file::mkdir);
    }

    /**
     * Rename a file in a privileged context.
     *
     * @param file the File to rename
     * @param dest the new name
     * @return same as File.renameTo
     */
    public static boolean fileRenameToPrivileged(final File file, final File dest) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> file.renameTo(dest));
    }

    /**
     * Delete a file in a privileged context
     *
     * @param file The File to delete
     * @return same as File.delete
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean fileDeletePrivileged(final File file) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) file::delete);
    }

    // endregion privileged actions
}
