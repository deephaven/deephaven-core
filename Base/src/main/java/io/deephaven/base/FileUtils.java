/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.ArrayList;

public class FileUtils {
    private final static FileFilter DIRECTORY_FILE_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory();
        }
    };
    private final static File[] EMPTY_DIRECTORY_ARRAY = new File[0];
    private final static FilenameFilter DIRECTORY_FILENAME_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return new File(dir, name).isDirectory();
        }
    };
    private final static String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * Cleans the specified path. All files and subdirectories in the path will be deleted. (ie
     * you'll be left with an empty directory).
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
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File f[] = file.listFiles();

            if (f != null) {
                for (File file1 : f) {
                    deleteRecursively(file1);
                }
            }
        }
        Assert.assertion(file.delete(), "file.delete()", file.getAbsolutePath(), "file");
    }

    /**
     * Move files accepted by a filter from their relative path under source to the same relative
     * path under destination. Creates missing destination subdirectories as needed.
     * 
     * @param source Must be a directory.
     * @param destination Must be a directory if it exists.
     * @param filter Applied to normal files, only. We recurse on directories automatically.
     * @param allowReplace If the destination regular file exists, do we replace it, or silently
     *        ignore it?
     */
    public static void moveRecursively(File source, File destination, @Nullable FileFilter filter,
        boolean allowReplace) {
        Require.neqNull(source, "source");
        Require.requirement(source.isDirectory(), "source.isDirectory()");
        Require.neqNull(destination, "destination");
        Require.requirement(!destination.exists() || destination.isDirectory(),
            "!destination.exists() || destination.isDirectory()");
        moveRecursivelyInternal(source, destination, new RecursingNormalFileFilter(filter),
            allowReplace);
    }

    private static void moveRecursivelyInternal(File source, File destination, FileFilter filter,
        boolean allowReplace) {
        final boolean sourceIsDirectory = source.isDirectory();
        if (sourceIsDirectory) {
            for (final File file : source.listFiles(filter)) {
                moveRecursivelyInternal(file, new File(destination, file.getName()), filter,
                    allowReplace);
            }
            return;
        }
        if (!allowReplace && destination.exists()) {
            return;
        }
        final File destinationParent = destination.getParentFile();
        if (!destinationParent.isDirectory()) {
            if (destinationParent.exists()) {
                throw new IllegalArgumentException("Destination parent "
                    + destinationParent.getAbsolutePath()
                    + " exists but is not a directory,  when moving " + source.getAbsolutePath());
            }
            if (!destinationParent.mkdirs()) {
                throw new RuntimeException("Failed to create missing destination parent directory "
                    + destinationParent.getAbsolutePath() + " when moving "
                    + source.getAbsolutePath());
            }
        }
        if (!source.renameTo(destination)) {
            throw new RuntimeException("Failed to move file " + source.getAbsolutePath() + " to "
                + destination.getAbsolutePath());
        }
    }

    /**
     * Recursive delete method that copes with .nfs files. Uses the file's parent as the trash
     * directory.
     * 
     * @param file
     */
    public static void deleteRecursivelyOnNFS(File file) {
        deleteRecursivelyOnNFS(new File(file.getParentFile(), '.' + file.getName() + ".trash"),
            file);
    }

    /**
     * Recursive delete method that copes with .nfs files.
     * 
     * @param trashFile Filename to move regular files to before deletion. .nfs files may be created
     *        in its parent directory.
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
                throw new RuntimeException("Failed to delete expected empty directory "
                    + fileToBeDeleted.getAbsolutePath());
            }
        } else if (fileToBeDeleted.exists()) {
            if (!fileToBeDeleted.renameTo(trashFile)) {
                throw new RuntimeException(
                    "Failed to move file " + fileToBeDeleted.getAbsolutePath()
                        + " to temporary location " + trashFile.getAbsolutePath());
            }
            if (!trashFile.delete()) {
                throw new RuntimeException(
                    "Failed to delete temporary location " + trashFile.getAbsolutePath()
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
     * I have no idea what to call this class. It accepts all directories, and normal files accepted
     * by its delegate filter.
     */
    private static class RecursingNormalFileFilter implements FileFilter {

        private final FileFilter normalFileFilter;

        private RecursingNormalFileFilter(FileFilter normalFileFilter) {
            this.normalFileFilter = normalFileFilter;
        }

        @Override
        public boolean accept(File pathname) {
            return pathname.isDirectory() || (pathname.isFile()
                && (normalFileFilter == null || normalFileFilter.accept(pathname)));
        }
    }
}
