package io.deephaven.db.util.file;

import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Base class for accessors that wrap a {@link FileHandle} with support for interruption and asynchronous close.
 */
public abstract class FileHandleAccessor {

    private final FileHandleFactory.FileToHandleFunction fileHandleCreator;
    protected final File file;

    protected volatile FileHandle fileHandle;

    /**
     * Create an accessor that gets handles for {@code file} from {@code fileHandleCreator}.
     *
     * @param fileHandleCreator The function used to make file handles
     * @param file The abstract path name to access
     */
    protected FileHandleAccessor(@NotNull final FileHandleFactory.FileToHandleFunction fileHandleCreator,
            @NotNull final File file) {
        this.fileHandleCreator = fileHandleCreator;
        this.file = Utils.fileGetAbsoluteFilePrivileged(file);
        fileHandle = makeHandle();
    }

    private FileHandle makeHandle() {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<FileHandle>) () -> {
                try {
                    return fileHandleCreator.invoke(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(this + ": makeHandle encountered exception", e);
                }
            });
        } catch (final PrivilegedActionException pae) {
            if (pae.getException() instanceof UncheckedIOException) {
                throw (UncheckedIOException) pae.getException();
            } else {
                throw new RuntimeException(pae.getException());
            }
        }
    }

    /**
     * Replace the file handle with a new one if the closed handle passed in is still current, and return the (possibly
     * changed) current value.
     *
     * @param previousLocalHandle The closed handle that calling code would like to replace
     * @return The current file handle, possibly newly created
     */
    protected final FileHandle refreshFileHandle(final FileHandle previousLocalHandle) {
        if (previousLocalHandle == fileHandle) {
            synchronized (this) {
                if (previousLocalHandle == fileHandle) {
                    fileHandle = makeHandle();
                }
            }
        }
        return fileHandle;
    }

    @Override
    public final String toString() {
        return file.toString();
    }
}
