//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.file;

import io.deephaven.base.stats.State;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * <p>
 * A representation of an open file. Designed to ensure predictable cleanup for open file descriptors.
 *
 * <p>
 * This class is basically just a wrapper around a {@link FileChannel} that only exposes some of its methods. It serves
 * two purposes:
 * <ol>
 * <li>It creates an extra layer of indirection between the FileChannel and application code, to allow for
 * reachability-sensitive cleanup.</li>
 * <li>It's a convenient place to add instrumentation and/or modified implementations when necessary.</li>
 * </ol>
 *
 * <p>
 * The current implementation adds a post-close procedure for integration with caches/trackers, and stats for all
 * operations.
 *
 * <p>
 * Note that positional methods, e.g. {@link #position()}, {@link #position(long)}, {@link #read(ByteBuffer)}, and
 * {@link #write(ByteBuffer)} may require external synchronization if used concurrently by more than one thread.
 */
public final class FileHandle implements SeekableByteChannel {

    private static final Value SIZE_DURATION_NANOS =
            Stats.makeItem("FileHandle", "sizeDurationNanos", State.FACTORY).getValue();
    private static final Value GET_POSITION_DURATION_NANOS =
            Stats.makeItem("FileHandle", "getPositionDurationNanos", State.FACTORY).getValue();
    private static final Value SET_POSITION_DURATION_NANOS =
            Stats.makeItem("FileHandle", "setPositionDurationNanos", State.FACTORY).getValue();
    private static final Value READ_DURATION_NANOS =
            Stats.makeItem("FileHandle", "readDurationNanos", State.FACTORY).getValue();
    private static final Value READ_SIZE_BYTES =
            Stats.makeItem("FileHandle", "readSizeBytes", State.FACTORY).getValue();
    private static final Value WRITE_DURATION_NANOS =
            Stats.makeItem("FileHandle", "writeDurationNanos", State.FACTORY).getValue();
    private static final Value WRITE_SIZE_BYTES =
            Stats.makeItem("FileHandle", "writeSizeBytes", State.FACTORY).getValue();
    private static final Value TRUNCATE_DURATION_NANOS =
            Stats.makeItem("FileHandle", "truncateDurationNanos", State.FACTORY).getValue();
    private static final Value FORCE_DURATION_NANOS =
            Stats.makeItem("FileHandle", "forceDurationNanos", State.FACTORY).getValue();

    static final String SAFETY_CHECK_PROPERTY = "FileHandle.safetyCheckEnabled";

    private static final boolean SAFETY_CHECK_ENABLED =
            Configuration.getInstance().getBooleanWithDefault(SAFETY_CHECK_PROPERTY, true);

    private final FileChannel fileChannel;
    private final Runnable postCloseProcedure;
    private final Object fileKey;

    /**
     * Creates a new file handle by wrapping up the results of an {@link FileChannel#open(Path, OpenOption...)}.
     *
     * <p>
     * If the {@link Runnable} created from {@code postCloseProcedureSupplier} throws an exception, that exception may
     * suppress {@link ClosedChannelException}s that trigger {@code postCloseProcedure} invocation.
     *
     * <p>
     * By default, the returned file handle will contain its {@link BasicFileAttributes#fileKey() file key} to act as a
     * safety check during refreshes of the file handle as part of {@link FileHandleAccessor}. To disable this safety
     * check, the configuration property {@value SAFETY_CHECK_PROPERTY} can be set to {@code false}.
     *
     * @param path The path to the file
     * @param postCloseProcedureSupplier A supplier for a procedure to invoke if it's detected that the
     *        {@link FileChannel} is closed - the procedure must be idempotent. The supplier will be called exactly once
     *        after the {@link FileChannel} has been successfully created.
     * @param options the open options
     * @return the file handle
     * @throws IOException if an IO exception occurs
     */
    public static FileHandle open(
            @NotNull final Path path,
            @NotNull final Supplier<Runnable> postCloseProcedureSupplier,
            @NotNull final OpenOption... options)
            throws IOException {
        final FileChannel fileChannel = FileChannel.open(path, options);
        final Object fileKey;
        if (!SAFETY_CHECK_ENABLED) {
            fileKey = null;
        } else {
            // Note: there is a chance for a race here (that the attributes read here are for a different file that what
            // is represented by fileChannel); Java does not provide an API for getting file attributes from an already
            // open FileChannel. (Arguably, this is something Java could provide in the future with an
            // https://linux.die.net/man/2/fstat call, which they already use internally in some cases.). That said, a
            // race here at worst will lead to our old behavior of not doing a safety check. If we really wanted to, we
            // could work around the race by sandwiching two readAttribute calls around the open call, and verifying
            // that the two readAttribute call keys were the same (and if not, retrying until success).
            final BasicFileAttributes attributes;
            try {
                attributes = Files.readAttributes(path, BasicFileAttributes.class);
            } catch (final RuntimeException e) {
                try {
                    fileChannel.close();
                } catch (final RuntimeException e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }
            // May be null here still
            fileKey = attributes.fileKey();
        }
        return new FileHandle(fileChannel, postCloseProcedureSupplier.get(), fileKey);
    }

    /**
     * <p>
     * Wrap the supplied {@link FileChannel}.
     * <p>
     * If the {@code postCloseProcedure} throws an exception, that exception may suppress
     * {@link ClosedChannelException}s that trigger {@code postCloseProcedure} invocation.
     *
     * @param fileChannel The {@link FileChannel}
     * @param postCloseProcedure A procedure to invoke if its detected that the {@link FileChannel} is closed - must be
     *        idempotent
     * @deprecated prefer {@link #open(Path, Supplier, OpenOption...)}
     */
    @Deprecated
    public FileHandle(@NotNull final FileChannel fileChannel, @NotNull final Runnable postCloseProcedure) {
        this(fileChannel, postCloseProcedure, null);
    }

    private FileHandle(
            @NotNull final FileChannel fileChannel,
            @NotNull final Runnable postCloseProcedure,
            @Nullable final Object fileKey) {
        this.fileChannel = Require.neqNull(fileChannel, "fileChannel");
        this.postCloseProcedure = Require.neqNull(postCloseProcedure, "postCloseProcedure");
        this.fileKey = fileKey;
    }

    /**
     * Checks if two file handles are equal based on their {@link BasicFileAttributes#fileKey()} on a best-effort basis.
     * This method should only return {@code false} when it is known that the file handles refer to different files -
     * that is, the bias is for this method to return {@code true}. The exact semantics of "file key equivalence" is
     * filesystem-dependant, but the general expectation is that UNIX filesystems will use <em>device ID</em> and
     * <em>inode</em> to accomplish this. (A UNIX filesystem may choose to re-use <em>device ID</em> and <em>inode</em>
     * after a file has been deleted. In this case, it is possible for two file handles that referenced two separate
     * files to have "file key equivalence", and is one of the reason why this method must be biased in the {@code true}
     * direction.)
     *
     * <p>
     * If the configuration property {@value #SAFETY_CHECK_PROPERTY} is {@code false}, this method will always return
     * {@code true}.
     *
     * @param other the other file handle
     * @return true if the file keys are equal
     */
    public boolean equalsFileKey(@NotNull final FileHandle other) {
        return Objects.equals(fileKey, other.fileKey);
    }

    FileChannel fileChannel() {
        return fileChannel;
    }

    Runnable postCloseProcedure() {
        return postCloseProcedure;
    }

    /**
     * <p>
     * Get the current size of the file.
     * <p>
     * See {@link FileChannel#size()}.
     *
     * @return The current size of the file
     */
    @Override
    public final long size() throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            try {
                return fileChannel.size();
            } finally {
                SIZE_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Get this file handle's position.
     * <p>
     * See {@link FileChannel#position()}.
     *
     * @return This file handle's position
     */
    @Override
    public final long position() throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            try {
                return fileChannel.position();
            } finally {
                GET_POSITION_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Advance the position of this file handle to the specified new position.
     * <p>
     * See {@link FileChannel#position(long)}.
     *
     * @param newPosition The new position
     * @return This file handle
     */
    @Override
    public final FileHandle position(long newPosition) throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            try {
                fileChannel.position(newPosition);
            } finally {
                SET_POSITION_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
        return this;
    }


    /**
     * <p>
     * Attempt to read {@code destination.remaining()} bytes, starting from {@code position} (0-indexed) in the file.
     * <p>
     * See {@link FileChannel#read(ByteBuffer, long)}.
     *
     * @param destination The destination to read to
     * @param position The position in the file to start reading from
     * @return The number of bytes read, or -1 if end of file is reached
     */
    public final int read(@NotNull final ByteBuffer destination, final long position) throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            final int sizeBytes = destination.remaining();
            try {
                return fileChannel.read(destination, position);
            } finally {
                READ_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
                READ_SIZE_BYTES.sample(sizeBytes);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Attempt to read {@code destination.remaining()} bytes, beginning at the handle's current position and updating
     * that position by the number of bytes read.
     * <p>
     * See {@link FileChannel#read(ByteBuffer)}.
     *
     * @param destination The destination to read to
     * @return The number of bytes read, or -1 of end of file is reached
     */
    @Override
    public final int read(@NotNull final ByteBuffer destination) throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            final int sizeBytes = destination.remaining();
            try {
                return fileChannel.read(destination);
            } finally {
                READ_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
                READ_SIZE_BYTES.sample(sizeBytes);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Attempt to write {@code source.remaining()} bytes, starting from {@code position} (0-indexed) in the file.
     * <p>
     * See {@link FileChannel#write(ByteBuffer, long)}.
     *
     * @param source The source to write from
     * 
     * @param position The position in the file to start writing at
     * @return The number of bytes written
     */
    public final int write(@NotNull final ByteBuffer source, final long position) throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            final int sizeBytes = source.remaining();
            try {
                return fileChannel.write(source, position);
            } finally {
                WRITE_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
                WRITE_SIZE_BYTES.sample(sizeBytes);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Attempt to write {@code source.remaining()} bytes to this file handle, beginning at the handle's current position
     * (which is first advanced to the end of the file, if the underlying {@link FileChannel} was opened with
     * {@link java.nio.file.StandardOpenOption#APPEND}), and updating that position by the number of bytes written.
     * <p>
     * See {@link FileChannel#write(ByteBuffer)}.
     *
     * @param source The source to write from
     * @return The number of bytes written
     */
    @Override
    public final int write(@NotNull final ByteBuffer source) throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            final int sizeBytes = source.remaining();
            try {
                return fileChannel.write(source);
            } finally {
                WRITE_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
                WRITE_SIZE_BYTES.sample(sizeBytes);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Truncate this file to the supplied size.
     * <p>
     * See {@link FileChannel#truncate(long)}.
     *
     * @param size The new size
     * @return This handle
     */
    @Override
    public final FileHandle truncate(final long size) throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            try {
                fileChannel.truncate(size);
            } finally {
                TRUNCATE_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
        return this;
    }

    /**
     * <p>
     * Force updates (including metadata) to the underlying file to be written to *local* storage.
     * <p>
     * See {@link FileChannel#force(boolean)}.
     */
    public final void force() throws IOException {
        try {
            final long startTimeNanos = System.nanoTime();
            try {
                fileChannel.force(true);
            } finally {
                FORCE_DURATION_NANOS.sample(System.nanoTime() - startTimeNanos);
            }
        } catch (ClosedChannelException e) {
            postCloseProcedure.run();
            throw e;
        }
    }

    /**
     * <p>
     * Tells whether this file handle is open.
     * <p>
     * See {@link FileChannel#isOpen()}.
     *
     * @return If the file handle is open
     */
    @Override
    public final boolean isOpen() {
        final boolean isOpen = fileChannel.isOpen();
        if (!isOpen) {
            postCloseProcedure.run();
        }
        return isOpen;
    }

    /**
     * <p>
     * Close this file handle and release underlying resources.
     * <p>
     * See {@link FileChannel#close()}.
     */
    @Override
    public final void close() throws IOException {
        try {
            fileChannel.close();
        } finally {
            postCloseProcedure.run();
        }
    }
}
