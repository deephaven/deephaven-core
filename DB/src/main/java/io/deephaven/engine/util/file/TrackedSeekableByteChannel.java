package io.deephaven.engine.util.file;

import io.deephaven.engine.table.impl.locations.TableDataException;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;

/**
 * <p>{@link SeekableByteChannel} wrapper around {@link FileHandle} with support for re-opening the handle in case it
 * has been closed asynchronously.
 * <p>Note the applications must be sure to use {@link #read(ByteBuffer)}, {@link #write(ByteBuffer)}, and
 * {@link #position(long)} in a thread safe manner, using external synchronization or other means.
 */
public final class TrackedSeekableByteChannel extends FileHandleAccessor implements SeekableByteChannel {

    private static final long CLOSED_SENTINEL = -1;

    private volatile long position;

    /**
     * Make a channel for a "local" file.
     *
     * @param fileHandleCreator The function used to make file handles
     * @param file              The abstract path name to wrap access to
     */
    public TrackedSeekableByteChannel(@NotNull final FileHandleFactory.FileToHandleFunction fileHandleCreator,
                                      @NotNull final File file) throws IOException {
        super(fileHandleCreator, file);
        initializePosition();
    }

    @Override
    public final int read(@NotNull final ByteBuffer destination) throws IOException {
        long localPosition = position;
        checkClosed(localPosition);
        if (!destination.hasRemaining()) {
            return 0;
        }
        FileHandle localHandle = fileHandle;
        int totalBytesRead = 0;
        while (true) {
            try {
                int bytesRead;
                do {
                    if ((bytesRead = localHandle.read(destination, localPosition)) != -1) {
                        localPosition += bytesRead;
                        position = localPosition;
                        totalBytesRead += bytesRead;
                    }
                } while (destination.hasRemaining() && bytesRead != -1);
                return totalBytesRead > 0 ? totalBytesRead : bytesRead;
            } catch (ClosedByInterruptException e) {
                throw new TableDataException(this + ": read interrupted", e);
            } catch (ClosedChannelException e) {
                // NB: This includes AsynchronousCloseException.
                localHandle = refreshFileHandle(localHandle);
            }
        }
    }

    @Override
    public final int write(@NotNull final ByteBuffer source) throws IOException {
        long localPosition = position;
        checkClosed(localPosition);
        if (!source.hasRemaining()) {
            return 0;
        }
        FileHandle localHandle = fileHandle;
        int totalBytesWritten = 0;
        while (true) {
            try {
                int bytesWritten;
                do {
                    if ((bytesWritten = localHandle.write(source, localPosition)) != -1) {
                        localPosition += bytesWritten;
                        position = localPosition;
                        totalBytesWritten += bytesWritten;
                    }
                } while (source.hasRemaining() && bytesWritten != -1);
                return totalBytesWritten > 0 ? totalBytesWritten : bytesWritten;
            } catch (ClosedByInterruptException e) {
                throw new TableDataException(this + ": write interrupted", e);
            } catch (ClosedChannelException e) {
                localHandle = refreshFileHandle(localHandle);
            }
        }
    }

    private void initializePosition() throws IOException {
        FileHandle localHandle = fileHandle;
        while (true) {
            try {
                position = localHandle.position();
                return;
            } catch (ClosedByInterruptException e) {
                throw new TableDataException(this + ": get position interrupted", e);
            } catch (ClosedChannelException e) {
                localHandle = refreshFileHandle(localHandle);
            }
        }
    }

    @Override
    public final long position() throws IOException {
        final long localPosition = position;
        checkClosed(localPosition);
        return localPosition;
    }

    @Override
    public final SeekableByteChannel position(final long newPosition) throws IOException {
        checkClosed(position);
        position = newPosition;
        return this;
    }

    @Override
    public final long size() throws IOException {
        checkClosed(position);
        FileHandle localHandle = fileHandle;
        while (true) {
            try {
                return localHandle.size();
            } catch (ClosedByInterruptException e) {
                throw new TableDataException(this + ": size interrupted", e);
            } catch (ClosedChannelException e) {
                localHandle = refreshFileHandle(localHandle);
            }
        }
    }

    @Override
    public final SeekableByteChannel truncate(final long size) throws IOException {
        checkClosed(position);
        FileHandle localHandle = fileHandle;
        while (true) {
            try {
                localHandle.truncate(size);
                if (position > size) {
                    position = size;
                }
                return this;
            } catch (ClosedByInterruptException e) {
                throw new TableDataException(this + ": onTruncate interrupted", e);
            } catch (ClosedChannelException e) {
                localHandle = refreshFileHandle(localHandle);
            }
        }
    }

    private static void checkClosed(final long position) throws ClosedChannelException {
        if (position == CLOSED_SENTINEL) {
            throw new ClosedChannelException();
        }
    }

    @Override
    public final boolean isOpen() {
        return position != CLOSED_SENTINEL;
    }

    @Override
    public final void close() throws IOException {
        position = CLOSED_SENTINEL;
        fileHandle.close();
    }
}
