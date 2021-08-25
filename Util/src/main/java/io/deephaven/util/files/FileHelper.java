package io.deephaven.util.files;

import io.deephaven.base.FileUtils;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.regex.Pattern;

import static io.deephaven.base.FileUtils.missingSafeListFiles;

/**
 * Utility/helper methods for file operations.
 */
public class FileHelper {

    /**
     * Not intended to be instantiated.
     */
    private FileHelper() {}

    /**
     * Get the canonical path for the given path string, converting IOExceptions to
     * UncheckIOException.
     *
     * @param path The file (as String) for which to get the canonical form.
     * @return the canonical file string.
     */
    public static String getCanonicalForm(String path) {
        try {
            return new File(path).getCanonicalPath();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Augment {@link FileUtils#deleteRecursivelyOnNFS(File)} with an exclude pattern. Files
     * matching the pattern will not be removed, nor will containing directories.
     *
     * @param file the file or folder to delete.
     * @param excludePattern don't delete files or folders matching this pattern.
     * @return true if any files were excluded (so caller will know if a directory is empty)
     * @throws FileDeletionException on any errors moving/renaming/deleting files
     */
    public static boolean deleteRecursivelyOnNFS(File file, String excludePattern) {
        Pattern pattern = excludePattern == null ? null : Pattern.compile(excludePattern);
        return deleteRecursivelyOnNFS(
            new File(file.getParentFile(), '.' + file.getName() + ".trash"), file, pattern);
    }

    /**
     * Augment {@link FileUtils#deleteRecursivelyOnNFS(File)} with an exclude pattern. Files
     * matching the pattern will not be removed, nor will containing directories.
     *
     * @param file the file or folder to delete.
     * @param excludePattern don't delete files or folders matching this pattern.
     * @return true if any files were excluded (so caller will know if a directory is empty)
     * @throws FileDeletionException on any errors moving/renaming/deleting files
     */
    public static boolean deleteRecursivelyOnNFS(File file, Pattern excludePattern) {
        return deleteRecursivelyOnNFS(
            new File(file.getParentFile(), '.' + file.getName() + ".trash"), file, excludePattern);
    }

    /**
     * Augment {@link FileUtils#deleteRecursivelyOnNFS(File, File)} with an exclude pattern. Files
     * matching the pattern will not be removed, nor will containing directories.
     * <p>
     * This is an implementation method for {@link #deleteRecursivelyOnNFS(File, String)}
     *
     * @param trashFile Filename to move regular files to before deletion. .nfs files may be created
     *        in its parent directory.
     * @param fileToBeDeleted File or directory at which to begin recursive deletion.
     * @param excludePattern don't delete files or folders matching this pattern.
     * @return true if any files were excluded (so caller will know if a directory is empty)
     * @throws FileDeletionException on any errors moving/renaming/deleting files
     */
    public static boolean deleteRecursivelyOnNFS(File trashFile, File fileToBeDeleted,
        @Nullable Pattern excludePattern) {
        if (excludePattern != null && excludePattern.matcher(fileToBeDeleted.getName()).matches()) {
            return true;
        }
        if (fileToBeDeleted.isDirectory()) {
            File contents[] = missingSafeListFiles(fileToBeDeleted);
            boolean excluded = false;
            for (File childFile : contents) {
                excluded = deleteRecursivelyOnNFS(trashFile, childFile, excludePattern) || excluded;
            }
            if (!excluded && !fileToBeDeleted.delete()) {
                throw new FileDeletionException("Failed to delete expected empty directory "
                    + fileToBeDeleted.getAbsolutePath());
            }
            return excluded;
        } else if (fileToBeDeleted.exists()) {
            if (!fileToBeDeleted.renameTo(trashFile)) {
                throw new FileDeletionException(
                    "Failed to move file " + fileToBeDeleted.getAbsolutePath()
                        + " to temporary location " + trashFile.getAbsolutePath());
            }
            if (!trashFile.delete()) {
                throw new FileDeletionException(
                    "Failed to delete temporary location " + trashFile.getAbsolutePath()
                        + " for file " + fileToBeDeleted.getAbsolutePath());
            }
            return false;
        }
        return true;
    }

    /**
     * Give callers of {@link #deleteRecursivelyOnNFS(File, Pattern)} a better exception to catch.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class FileDeletionException extends RuntimeException {
        public FileDeletionException() {
            super();
        }

        public FileDeletionException(String message) {
            super(message);
        }

        public FileDeletionException(String message, Throwable cause) {
            super(message, cause);
        }

        public FileDeletionException(Throwable cause) {
            super(cause);
        }
    }
}
