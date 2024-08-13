//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.trackedfile;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.CompletableOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

/**
 * A {@link CompletableOutputStream} that writes to a temporary shadow file paths in the same directory to prevent
 * overwriting any existing data in case of failure.
 */
public class CompletableLocalOutputStream extends CompletableOutputStream {

    private final File firstCreatedDir;
    private final File destFile;
    private final File shadowDestFile;
    private final OutputStream shadowDelegateStream; // Writes to the shadow file

    private boolean done;
    private boolean closed;
    private boolean installedShadowFiles;

    private static final Logger log = LoggerFactory.getLogger(CompletableLocalOutputStream.class);

    CompletableLocalOutputStream(
            @NotNull final File destFile,
            @NotNull final TrackedSeekableChannelsProvider provider,
            final int bufferSizeHint) throws IOException {
        this.firstCreatedDir = prepareDestinationFileLocation(destFile);
        this.destFile = destFile;
        deleteBackupFile(destFile);
        this.shadowDestFile = getShadowFile(destFile);
        this.shadowDelegateStream = new BufferedOutputStream(Channels.newOutputStream(
                provider.getWriteChannel(shadowDestFile)), bufferSizeHint);
    }

    @Override
    public void write(int b) throws IOException {
        verifyNotClosed();
        shadowDelegateStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        verifyNotClosed();
        shadowDelegateStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        verifyNotClosed();
        shadowDelegateStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        verifyNotClosed();
        shadowDelegateStream.flush();
    }

    public void done() {
        done = true;
    }

    public void complete() throws IOException {
        done();
        shadowDelegateStream.close();
        installShadowFile(destFile, shadowDestFile);
        installedShadowFiles = true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        shadowDelegateStream.close();
        deleteBackupFileNoExcept(destFile);
        closed = true;
    }

    public void rollback() {
        if (installedShadowFiles) {
            rollbackShadowFiles(destFile);
        }
        // noinspection ResultOfMethodCallIgnored
        shadowDestFile.delete();
        if (firstCreatedDir != null) {
            log.error().append("Cleaning up potentially incomplete table destination path starting from ")
                    .append(firstCreatedDir.getAbsolutePath()).endl();
            FileUtils.deleteRecursivelyOnNFS(firstCreatedDir);
        }
    }

    ////////////// Helper methods /////////////

    private void verifyNotClosed() {
        if (done) {
            throw new UncheckedDeephavenException("Write failed because the stream is already marked done");
        }
        if (closed) {
            throw new UncheckedDeephavenException("Write failed because the stream is already closed");
        }
    }

    /**
     * Delete any old backup files created for this destination, and throw an exception on failure.
     */
    private static void deleteBackupFile(@NotNull final File destFile) {
        if (!deleteBackupFileNoExcept(destFile)) {
            throw new UncheckedDeephavenException(
                    String.format("Failed to delete backup file at %s", getBackupFile(destFile)));
        }
    }

    /**
     * Delete any old backup files created for this destination with no exception in case of failure.
     */
    private static boolean deleteBackupFileNoExcept(@NotNull final File destFile) {
        final File backupDestFile = getBackupFile(destFile);
        if (backupDestFile.exists() && !backupDestFile.delete()) {
            log.error().append("Error in deleting backup file at path ")
                    .append(backupDestFile.getAbsolutePath())
                    .endl();
            return false;
        }
        return true;
    }

    private static File getBackupFile(final File destFile) {
        return new File(destFile.getParent(), ".OLD_" + destFile.getName());
    }

    private static File getShadowFile(final File destFile) {
        return new File(destFile.getParent(), ".NEW_" + destFile.getName());
    }

    /**
     * Make any missing ancestor directories of {@code destination}.
     *
     * @param destFile The destination file
     * @return The first created directory, or null if no directories were made.
     */
    @Nullable
    private static File prepareDestinationFileLocation(@NotNull final File destFile) {
        final File destination = destFile.getAbsoluteFile();
        if (destination.exists()) {
            if (destination.isDirectory()) {
                throw new UncheckedDeephavenException(
                        String.format("Destination %s exists and is a directory", destination));
            }
            if (!destination.canWrite()) {
                throw new UncheckedDeephavenException(
                        String.format("Destination %s exists but is not writable", destination));
            }
            return null;
        }
        final File firstParent = destination.getParentFile();
        if (firstParent.isDirectory()) {
            if (firstParent.canWrite()) {
                return null;
            }
            throw new UncheckedDeephavenException(
                    String.format("Destination %s has non writable parent directory", destination));
        }
        File firstCreated = firstParent;
        File parent;
        for (parent = destination.getParentFile(); parent != null && !parent.exists(); parent =
                parent.getParentFile()) {
            firstCreated = parent;
        }
        if (parent == null) {
            throw new IllegalArgumentException(
                    String.format("Can't find any existing parent directory for destination path: %s", destination));
        }
        if (!parent.isDirectory()) {
            throw new IllegalArgumentException(
                    String.format("Existing parent file %s of %s is not a directory", parent, destination));
        }
        if (!firstParent.mkdirs()) {
            throw new UncheckedDeephavenException("Couldn't (re)create destination directory " + firstParent);
        }
        return firstCreated;
    }


    /**
     * Backup any existing files at destination and rename the shadow file to destination file.
     */
    private static void installShadowFile(@NotNull final File destFile, @NotNull final File shadowDestFile) {
        final File backupDestFile = getBackupFile(destFile);
        if (destFile.exists() && !destFile.renameTo(backupDestFile)) {
            throw new UncheckedDeephavenException(
                    String.format("Failed to install shadow file at %s because a file already exists at the path " +
                            "which " + "couldn't be renamed to %s", destFile.getAbsolutePath(),
                            backupDestFile.getAbsolutePath()));
        }
        if (!shadowDestFile.exists()) {
            throw new UncheckedDeephavenException(
                    String.format("Failed to install shadow file at %s because shadow file doesn't exist at %s",
                            destFile.getAbsolutePath(), shadowDestFile.getAbsolutePath()));
        }
        if (!shadowDestFile.renameTo(destFile)) {
            throw new UncheckedDeephavenException(String.format(
                    "Failed to install shadow file at %s because couldn't rename temporary shadow file from %s to %s",
                    destFile.getAbsolutePath(), shadowDestFile.getAbsolutePath(), destFile.getAbsolutePath()));
        }
    }

    /**
     * Roll back any changes made in the {@link #installShadowFile} in best-effort manner. This method is a no-op if the
     * destination is not a file URI.
     */
    private static void rollbackShadowFiles(@NotNull final File destFile) {
        final File backupDestFile = getBackupFile(destFile);
        final File shadowDestFile = getShadowFile(destFile);
        destFile.renameTo(shadowDestFile);
        backupDestFile.renameTo(destFile);
    }
}
