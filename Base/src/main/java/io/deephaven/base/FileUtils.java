//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class FileUtils {
    private final static FileFilter DIRECTORY_FILE_FILTER = File::isDirectory;
    private final static File[] EMPTY_DIRECTORY_ARRAY = new File[0];
    private final static FilenameFilter DIRECTORY_FILENAME_FILTER = (dir, name) -> new File(dir, name).isDirectory();
    private final static String[] EMPTY_STRING_ARRAY = new String[0];

    public static final char URI_SEPARATOR_CHAR = '/';

    public static final String URI_SEPARATOR = "" + URI_SEPARATOR_CHAR;

    public static final String REPEATED_URI_SEPARATOR = URI_SEPARATOR + URI_SEPARATOR;

    public static final Pattern REPEATED_URI_SEPARATOR_PATTERN = Pattern.compile("//+");

    public static final String FILE_URI_SCHEME = "file";

    /**
     * Cleans the specified path. All files and subdirectories in the path will be deleted. (ie you'll be left with an
     * empty directory).
     *
     * @param path The path to clean
     */
    public static void cleanDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    cleanDirectory(files[i]);
                }
                if (!files[i].delete()) {
                    throw new RuntimeException("Couldn't delete " + files[i].getAbsolutePath());
                }
            }
        }
    }

    public static void deleteRecursively(File file) {
        if (!Files.exists(file.toPath(), LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (file.isDirectory()) {
            final File[] files = file.listFiles();

            if (files != null) {
                for (final File child : files) {
                    deleteRecursively(child);
                }
            }
        }
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not delete file: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * Move files accepted by a filter from their relative path under source to the same relative path under
     * destination. Creates missing destination subdirectories as needed.
     *
     * @param source Must be a directory.
     * @param destination Must be a directory if it exists.
     * @param filter Applied to normal files, only. We recurse on directories automatically.
     * @param allowReplace If the destination regular file exists, do we replace it, or silently ignore it?
     */
    public static void moveRecursively(File source, File destination, @Nullable FileFilter filter,
            boolean allowReplace) {
        Require.neqNull(source, "source");
        Require.requirement(source.isDirectory(), "source.isDirectory()");
        Require.neqNull(destination, "destination");
        Require.requirement(!destination.exists() || destination.isDirectory(),
                "!destination.exists() || destination.isDirectory()");
        moveRecursivelyInternal(source, destination, new RecursingNormalFileFilter(filter), allowReplace);
    }

    private static void moveRecursivelyInternal(File source, File destination, FileFilter filter,
            boolean allowReplace) {
        final boolean sourceIsDirectory = source.isDirectory();
        if (sourceIsDirectory) {
            for (final File file : source.listFiles(filter)) {
                moveRecursivelyInternal(file, new File(destination, file.getName()), filter, allowReplace);
            }
            return;
        }
        if (!allowReplace && destination.exists()) {
            return;
        }
        final File destinationParent = destination.getParentFile();
        if (!destinationParent.isDirectory()) {
            if (destinationParent.exists()) {
                throw new IllegalArgumentException("Destination parent " + destinationParent.getAbsolutePath()
                        + " exists but is not a directory,  when moving " + source.getAbsolutePath());
            }
            if (!destinationParent.mkdirs()) {
                throw new RuntimeException("Failed to create missing destination parent directory "
                        + destinationParent.getAbsolutePath() + " when moving " + source.getAbsolutePath());
            }
        }
        if (!source.renameTo(destination)) {
            throw new RuntimeException(
                    "Failed to move file " + source.getAbsolutePath() + " to " + destination.getAbsolutePath());
        }
    }

    /**
     * Recursive delete method that copes with .nfs files. Uses the file's parent as the trash directory.
     *
     * @param file
     */
    public static void deleteRecursivelyOnNFS(File file) {
        deleteRecursivelyOnNFS(new File(file.getParentFile(), '.' + file.getName() + ".trash"), file);
    }

    /**
     * Recursive delete method that copes with .nfs files.
     *
     * @param trashFile Filename to move regular files to before deletion. .nfs files may be created in its parent
     *        directory.
     * @param fileToBeDeleted File or directory at which to begin recursive deletion.
     */
    public static void deleteRecursivelyOnNFS(final File trashFile, final File fileToBeDeleted) {
        if (fileToBeDeleted.isDirectory()) {
            File contents[] = fileToBeDeleted.listFiles();
            if (contents != null) {
                for (File childFile : contents) {
                    deleteRecursivelyOnNFS(trashFile, childFile);
                }
            }
            if (!fileToBeDeleted.delete()) {
                throw new RuntimeException(
                        "Failed to delete expected empty directory " + fileToBeDeleted.getAbsolutePath());
            }
        } else if (fileToBeDeleted.exists()) {
            if (!fileToBeDeleted.renameTo(trashFile)) {
                throw new RuntimeException("Failed to move file " + fileToBeDeleted.getAbsolutePath()
                        + " to temporary location " + trashFile.getAbsolutePath());
            }
            if (!trashFile.delete()) {
                throw new RuntimeException("Failed to delete temporary location " + trashFile.getAbsolutePath()
                        + " for file " + fileToBeDeleted.getAbsolutePath());
            }
        }
    }

    /**
     * Scan directory recursively to find all files
     *
     * @param dir
     * @return
     */
    public static File[] findAllFiles(File dir) {
        ArrayList<File> results = new ArrayList<File>();
        __findAllSubDirectories(results, dir);
        return results.toArray(new File[results.size()]);
    }

    private static void __findAllSubDirectories(ArrayList<File> results, File dir) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory())
                __findAllSubDirectories(results, f);
            else
                results.add(f);
        }
    }

    public static File[] missingSafeListFiles(File directory) {
        final File[] result = Require.neqNull(directory, "directory").listFiles();
        return result == null ? EMPTY_DIRECTORY_ARRAY : result;
    }

    public static File[] missingSafeListFiles(File directory, FileFilter filter) {
        final File[] result = Require.neqNull(directory, "directory").listFiles(filter);
        return result == null ? EMPTY_DIRECTORY_ARRAY : result;
    }

    public static File[] missingSafeListSubDirectories(File directory) {
        return missingSafeListFiles(directory, DIRECTORY_FILE_FILTER);
    }

    public static String[] missingSafeListFilenames(File directory) {
        final String[] result = Require.neqNull(directory, "directory").list();
        return result == null ? EMPTY_STRING_ARRAY : result;
    }

    public static String[] missingSafeListFilenames(File directory, FilenameFilter filter) {
        final String[] result = Require.neqNull(directory, "directory").list(filter);
        return result == null ? EMPTY_STRING_ARRAY : result;
    }

    public static String[] missingSafeListSubDirectoryNames(File directory) {
        return missingSafeListFilenames(directory, DIRECTORY_FILENAME_FILTER);
    }

    public static String readTextFile(File txtFile) throws IOException {
        if (!txtFile.exists()) {
            throw new IllegalArgumentException("File " + txtFile.getName() + " doesn't exist.");
        }
        if (txtFile.length() > Integer.MAX_VALUE) {
            throw new RuntimeException("File " + txtFile.getName() + " exceeds the 2 GB limit.");
        }
        StringBuilder buffer = new StringBuilder((int) txtFile.length());
        InputStream in = new FileInputStream(txtFile);

        byte[] byteBuffer = new byte[(int) txtFile.length()];
        in.read(byteBuffer);
        buffer.append(new String(byteBuffer));

        in.close();
        return buffer.toString().trim();
    }

    public static String readTextFile(InputStream txtFile) throws IOException {
        StringBuilder buffer = new StringBuilder();
        byte[] byteBuffer = new byte[65536];
        int readAmount;
        readAmount = txtFile.read(byteBuffer);
        while (readAmount > 0) {
            buffer.append(new String(byteBuffer, 0, readAmount));
            readAmount = txtFile.read(byteBuffer);
        }

        txtFile.close();
        return buffer.toString().trim();
    }

    /**
     * I have no idea what to call this class. It accepts all directories, and normal files accepted by its delegate
     * filter.
     */
    private static class RecursingNormalFileFilter implements FileFilter {

        private final FileFilter normalFileFilter;

        private RecursingNormalFileFilter(FileFilter normalFileFilter) {
            this.normalFileFilter = normalFileFilter;
        }

        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory()
                    || (pathname.isFile() && (normalFileFilter == null || normalFileFilter.accept(pathname)));
        }
    }

    /**
     * Take the file source path or URI string and convert it to a URI object. Any unnecessary path separators will be
     * removed. The URI object will always be {@link URI#isAbsolute() absolute}, i.e., will always have a scheme.
     *
     * @param source The file source path or URI
     * @param isDirectory Whether the source is a directory
     * @return The URI object
     */
    public static URI convertToURI(final String source, final boolean isDirectory) {
        if (source.isEmpty()) {
            throw new IllegalArgumentException("Cannot convert empty source to URI");
        }
        URI uri;
        try {
            uri = new URI(source);
            if (uri.getScheme() == null) {
                // Convert to a "file" URI
                return convertToURI(new File(source), isDirectory);
            }
            if (uri.getScheme().equals(FILE_URI_SCHEME)) {
                return convertToURI(new File(uri), isDirectory);
            }
            String path = uri.getPath();
            final boolean endsWithSlash = !path.isEmpty() && path.charAt(path.length() - 1) == URI_SEPARATOR_CHAR;
            if (!isDirectory && endsWithSlash) {
                throw new IllegalArgumentException("Non-directory URI should not end with a slash: " + uri);
            }
            boolean isUpdated = false;
            if (isDirectory && !endsWithSlash) {
                path = path + URI_SEPARATOR_CHAR;
                isUpdated = true;
            }
            // Replace two or more consecutive slashes in the path with a single slash
            if (path.contains(REPEATED_URI_SEPARATOR)) {
                path = REPEATED_URI_SEPARATOR_PATTERN.matcher(path).replaceAll(URI_SEPARATOR);
                isUpdated = true;
            }
            if (isUpdated) {
                uri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, uri.getQuery(),
                        uri.getFragment());
            }
        } catch (final URISyntaxException e) {
            // If the URI is invalid, assume it's a file path
            return convertToURI(new File(source), isDirectory);
        }
        return uri;
    }

    /**
     * Takes a file and convert it to a URI object with {@code "file"} scheme. This method is preferred instead of
     * {@link File#toURI()} because {@link File#toURI()} internally calls {@link File#isDirectory()}, which typically
     * invokes the {@code stat} system call, resulting in filesystem metadata access.
     *
     * @param file The file
     * @param isDirectory Whether the source file is a directory
     * @return The URI object
     */
    public static URI convertToURI(final File file, final boolean isDirectory) {
        String absPath = file.getAbsolutePath();
        if (File.separatorChar != URI_SEPARATOR_CHAR) {
            absPath = absPath.replace(File.separatorChar, URI_SEPARATOR_CHAR);
        }
        if (isDirectory && absPath.charAt(absPath.length() - 1) != URI_SEPARATOR_CHAR) {
            absPath = absPath + URI_SEPARATOR_CHAR;
        }
        if (absPath.charAt(0) != URI_SEPARATOR_CHAR) {
            absPath = URI_SEPARATOR_CHAR + absPath;
            // ^This is especially useful for Windows where the absolute path does not start with a slash.
            // For example, for absolute path "C:\path\to\file", the URI would be "file:/C:/path/to/file".
        }
        try {
            return new URI(FILE_URI_SCHEME, null, absPath, null);
        } catch (final URISyntaxException e) {
            throw new IllegalStateException("Failed to convert file to URI: " + file, e);
        }
    }

    /**
     * Takes a path and convert it to a URI object with {@code "file"} scheme. This method is preferred instead of
     * {@link Path#toUri()} because {@link Path#toUri()} internally invokes the {@code stat} system call, resulting in
     * filesystem metadata access.
     *
     * @param path The path
     * @param isDirectory Whether the file is a directory
     * @return The URI object
     */
    public static URI convertToURI(final Path path, final boolean isDirectory) {
        return convertToURI(path.toFile(), isDirectory);
    }
}
