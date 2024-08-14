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
class LocalCompletableOutputStream extends CompletableOutputStream {

    private static final Logger log = LoggerFactory.getLogger(LocalCompletableOutputStream.class);

    private enum State {
        OPEN, DONE, COMPLETED, ROLLED_BACK
    }

    private final File firstCreatedDir;
    private final File destFile;
    private final File shadowDestFile;
    private final OutputStream shadowDelegateStream; // Writes to the shadow file

    private State state;

    LocalCompletableOutputStream(
            @NotNull final File destFile,
            @NotNull final TrackedSeekableChannelsProvider provider,
            final int bufferSizeHint) throws IOException {
        this.firstCreatedDir = prepareDestinationFileLocation(destFile);
        this.destFile = destFile;
        deleteBackupFile(destFile);
        this.shadowDestFile = getShadowFile(destFile);
        this.shadowDelegateStream = new BufferedOutputStream(Channels.newOutputStream(
                provider.getWriteChannel(shadowDestFile)), bufferSizeHint);
        this.state = State.OPEN;
    }

    @Override
    public void write(int b) throws IOException {
        verifyOpen();
        shadowDelegateStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        verifyOpen();
        shadowDelegateStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        verifyOpen();
        shadowDelegateStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        verifyOpen();
        shadowDelegateStream.flush();
    }

    public void done() throws IOException {
        if (state == State.DONE) {
            return;
        }
        if (state != State.OPEN) {
            throw new IOException("Cannot mark stream as done for file " + destFile.getAbsolutePath() + " because " +
                    "stream in state " + state + " instead of OPEN");
        }
        flush();
        state = State.DONE;
    }

    public void complete() throws IOException {
        if (state == State.COMPLETED) {
            return;
        }
        done();
        shadowDelegateStream.close();
        installShadowFile(destFile, shadowDestFile);
        state = State.COMPLETED;
    }

    @Override
    public void rollback() {
        if (state == State.ROLLED_BACK) {
            return;
        }
        if (state == State.COMPLETED) {
            rollbackShadowFiles(destFile);
        }
        // noinspection ResultOfMethodCallIgnored
        shadowDestFile.delete();
        if (firstCreatedDir != null) {
            log.error().append("Cleaning up potentially incomplete table destination path starting from ")
                    .append(firstCreatedDir.getAbsolutePath()).endl();
            FileUtils.deleteRecursivelyOnNFS(firstCreatedDir);
        }
        state = State.ROLLED_BACK;
    }

    @Override
    public void close() throws IOException {
        if (state == State.ROLLED_BACK) {
            return;
        }
        if (state != State.COMPLETED) {
            rollback();
            return;
        }
        deleteBackupFileNoExcept(destFile);
    }

    ////////////// Helper methods /////////////

    private void verifyOpen() throws IOException {
        if (state != State.OPEN) {
            throw new IOException("Cannot write to stream for file " + destFile.getAbsolutePath() + " because stream " +
                    "in state " + state + " instead of OPEN");
        }
    }

    /**
     * Delete any old backup files created for this destination, and throw an exception on failure.
     */
    private static void deleteBackupFile(@NotNull final File destFile) {
        if (!deleteBackupFileNoExcept(destFile)) {
            throw new UncheckedDeephavenException(
                    String.format("Failed to delete backup file at %s", getBackupFile(destFile).getAbsolutePath()));
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
     * @param destination The destination file
     * @return The first created directory, or null if no directories were made.
     */
    @Nullable
    private static File prepareDestinationFileLocation(@NotNull File destination) {
        destination = destination.getAbsoluteFile();
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
                            "which couldn't be renamed to %s", destFile.getAbsolutePath(),
                            backupDestFile.getAbsolutePath()));
        }
        if (!shadowDestFile.renameTo(destFile)) {
            throw new UncheckedDeephavenException(String.format(
                    "Failed to install shadow file at %s because couldn't rename temporary shadow file from %s to %s",
                    destFile.getAbsolutePath(), shadowDestFile.getAbsolutePath(), destFile.getAbsolutePath()));
        }
    }

    /**
     * Roll back any changes made in the {@link #installShadowFile} in best-effort manner.
     */
    private static void rollbackShadowFiles(@NotNull final File destFile) {
        final File backupDestFile = getBackupFile(destFile);
        final File shadowDestFile = getShadowFile(destFile);
        destFile.renameTo(shadowDestFile);
        backupDestFile.renameTo(destFile);
    }
}
