#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import io


def _get_encoding_or_utf8(stream) -> str:
    """
    Helper to read the encoding from a stream. The stream implementation may or may not have an
    encoding property, and if it has one it might be None - if absent or None, use utf8, otherwise
    return the actual encoding value.

    Known cases:
     - "no encoding attr' - jpy doesn't include this attr for its built-in stdout/stderr impls
     - "encoding attr is None" - xmlrunner has an attribute, but it has a None value
     - other cases seem to provide a real value.
    :param stream: the stream to ask for its encoding
    :return: the encoding to use
    """
    return getattr(stream, 'encoding', 'UTF-8') or 'UTF-8'


class TeeStream(io.TextIOBase):
    """TextIOBase subclass that splits output between a delegate instance and a set of lambdas.

    Can delegate some calls to the provided stream but actually write only to the lambdas. Useful to adapt any existing
    file-like object (such as sys.stdout or sys.stderr), but actually let some other processing take place before
    writing.
    """

    @classmethod
    def split(cls, py_stream, java_stream):
        encoding = _get_encoding_or_utf8(py_stream)
        return TeeStream(
            orig_stream=py_stream,
            should_write_to_orig_stream=True,
            write_func=lambda t: java_stream.write(bytes(t, encoding)),
            flush_func=lambda: java_stream.flush(),
            close_func=lambda: java_stream.close()
        )

    @classmethod
    def redirect(cls, py_stream, java_stream):
        encoding = _get_encoding_or_utf8(py_stream)
        return TeeStream(
            orig_stream=py_stream,
            should_write_to_orig_stream=False,
            write_func=lambda t: java_stream.write(bytes(t, encoding)),
            flush_func=lambda: java_stream.flush(),
            close_func=lambda: java_stream.close()
        )

    def __init__(self, orig_stream, should_write_to_orig_stream, write_func, flush_func, close_func):
        """Creates a new TeeStream to let output be written from out place, but be sent to multiple places.

        Ideally, the stream would be passed as more funcs, but we have to manage correctly propagating certain non-java
        details like encoding and isatty.

        Args:
            orig_stream: the underlying python stream to use as a prototype
            should_write_to_orig_stream: True to delegate writes to the original stream, False to only write to the
              lambdas that follow
            write_func: a function to call when data should be written
            flush_func: a function to call when data should be flushed
            close_func: a function to call when the stream should be closed
        """
        self._stream = orig_stream
        self.should_write_to_orig_stream = should_write_to_orig_stream
        self.write_func = write_func
        self.flush_func = flush_func
        self.close_func = close_func

    def isatty(self):
        if not hasattr(self._stream, "isatty"):
            return False
        return self._stream.isatty()

    def fileno(self):
        if not hasattr(self._stream, "fileno"):
            return None
        return self._stream.fileno()

    @property
    def encoding(self):
        return _get_encoding_or_utf8(self._stream)

    def write(self, string):
        self.write_func(string)
        if self.should_write_to_orig_stream:
            self._stream.write(string)

    def flush(self):
        self.flush_func()
        if self.should_write_to_orig_stream:
            self._stream.flush()

    def close(self):
        super().close()
        self.close_func()
        if self.should_write_to_orig_stream:
            self._stream.close()

    def __del__(self):
        # Do nothing, override superclass which would call close()
        pass

    def set_parent(self, parent):
        # Parent needs to be set on the original stream so that output shows in proper cell
        self._stream.set_parent(parent)
