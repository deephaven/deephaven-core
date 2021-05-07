package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * <p>Lazily-initialized FileAccessor implementation.
 * <p>Defers initialization costs, e.g. file handle allocation or size-buffer reading whenever possible.
 */
public class LazyFileAccessor implements FileAccessor {

    private Supplier<FileAccessor> factory;

    private volatile FileAccessor internalFileAccessor;

    @SuppressWarnings("WeakerAccess")
    public LazyFileAccessor(@NotNull final Supplier<FileAccessor> factory) {
        this.factory = Require.neqNull(factory, "factory");
    }

    private void checkInternalFileAccessor() {
        if (internalFileAccessor == null) {
            synchronized (this) {
                if (internalFileAccessor == null) {
                    internalFileAccessor = factory.get();
                    factory = null;
                }
            }
        }
    }

    @Override
    public final long size(final long requiredSize) {
        checkInternalFileAccessor();
        return internalFileAccessor.size(requiredSize);
    }

    @Override
    public final int read(@NotNull final ByteBuffer buffer, final long position) {
        checkInternalFileAccessor();
        return internalFileAccessor.read(buffer, position);
    }

    @Override
    public int write(@NotNull final ByteBuffer buffer, final long position) {
        checkInternalFileAccessor();
        return internalFileAccessor.write(buffer, position);
    }

    @Override
    public void truncate(final long size) {
        checkInternalFileAccessor();
        internalFileAccessor.truncate(size);
    }

    @Override
    public void force() {
        checkInternalFileAccessor();
        internalFileAccessor.force();
    }

    @Override
    public FileAccessor getOffsetView(final long startOffset) {
        return new LazyFileAccessor(() -> {
            checkInternalFileAccessor();
            return internalFileAccessor.getOffsetView(startOffset);
        });
    }

    @Override
    public String getImplementationName() {
        return "LazyFileAccessor";
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        if (internalFileAccessor == null) {
            return logOutput.append(getImplementationName()).append("[initialization deferred]");
        }
        return logOutput.append(internalFileAccessor);
    }

    @Override
    public String toString() {
        if (internalFileAccessor == null) {
            return getImplementationName() + "[initialization deferred]";
        }
        return internalFileAccessor.toString();
    }
}
