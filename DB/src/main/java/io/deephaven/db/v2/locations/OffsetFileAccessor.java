package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 *
 * Wrap an underlying FileAccessor, providing an offset view into the underlying accessor's data.
 * The offset view will be writable if and only if the underlying FileAccessor is writable.
 */
@SuppressWarnings("WeakerAccess")
public class OffsetFileAccessor<UFAT extends FileAccessor> implements FileAccessor {

    protected final UFAT underlyingFileAccessor;
    protected final long startOffset;

    /**
     * @param underlyingFileAccessor The file accessor to wrap
     * @param startOffset The offset into the underlying file accessor's data
     */
    public OffsetFileAccessor(@NotNull final UFAT underlyingFileAccessor, final long startOffset) {
        this.underlyingFileAccessor = Require.neqNull(underlyingFileAccessor, "underlyingFileAccessor");
        this.startOffset = Require.geqZero(startOffset, "startOffset");
    }

    @Override
    public long size(final long requiredSize) {
        return Math.max(0, underlyingFileAccessor.size(requiredSize == NULL_REQUIRED_SIZE ? NULL_REQUIRED_SIZE : requiredSize + startOffset) - startOffset);
    }

    @Override
    public int read(final ByteBuffer buffer, final long position) {
        return underlyingFileAccessor.read(buffer, position + startOffset);
    }

    @Override
    public int write(@NotNull final ByteBuffer buffer, final long position) {
        return underlyingFileAccessor.write(buffer, position + startOffset);
    }

    @Override
    public void truncate(final long size) {
        underlyingFileAccessor.truncate(size + startOffset);
    }

    @Override
    public void force() {
        underlyingFileAccessor.force();
    }

    @Override
    public FileAccessor getOffsetView(final long startOffset) {
        return new OffsetFileAccessor<>(underlyingFileAccessor, this.startOffset + startOffset);
    }

    @Override
    public String getImplementationName() {
        return "OffsetFileAccessor";
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getImplementationName())
                .append('[').append(underlyingFileAccessor)
                .append(',').append(startOffset)
                .append(']');
    }

    @Override
    public String toString() {
        return getImplementationName()
                + '[' + underlyingFileAccessor
                + ',' + startOffset
                + ']';
    }
}
