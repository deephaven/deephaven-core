package io.deephaven.integrations.python;

import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * Simple output stream that redirects all writes to a python io.TextIOBase type.
 */
public class PyLogOutputStream extends OutputStream {
    private final Supplier<PyObject> rawIoBaseSupplier;

    public PyLogOutputStream(Supplier<PyObject> rawIoBaseSupplier) {
        this.rawIoBaseSupplier = rawIoBaseSupplier;
    }

    @Override
    public void write(int i) throws IOException {
        write(new byte[] {(byte) i});
    }

    @Override
    public void write(@NotNull byte[] b) throws IOException {
        // TODO (deephaven#2793) switch to modern method overloads when jpy#87 is fixed
        rawIoBaseSupplier.get().callMethod("write", new String(b));
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
        byte[] buffer = new byte[len];
        System.arraycopy(b, off, buffer, 0, len);
        write(buffer);
    }

    @Override
    public void flush() throws IOException {
        // TODO (deephaven#2793) switch to modern method overloads when jpy#87 is fixed
        rawIoBaseSupplier.get().callMethod("flush");
    }

    @Override
    public void close() throws IOException {
        // TODO (deephaven#2793) switch to modern method overloads when jpy#87 is fixed
        rawIoBaseSupplier.get().callMethod("close");
    }
}
