import io


class TeeStream(io.TextIOBase):
    """TextIOBase subclass that splits output between a delegate instance and a set of lambdas.

    Can delegate some calls to the provided stream but actually write only to the lambdas. Useful to adapt any existing
    file-like object (such as sys.stdout or sys.stderr), but actually let some other processing take place before
    writing.
    """

    @classmethod
    def split(cls, py_stream, java_stream):
        if hasattr(py_stream, "encoding"):
            encoding = py_stream.encoding
        else:
            encoding = 'UTF-8'
        return TeeStream(
            orig_stream=py_stream,
            should_write_to_orig_stream=True,
            write_func=lambda t: java_stream.write(bytes(t, encoding)),
            flush_func=lambda: java_stream.flush(),
            close_func=lambda: java_stream.close()
        )

    @classmethod
    def redirect(cls, py_stream, java_stream):
        if hasattr(py_stream, "encoding"):
            encoding = py_stream.encoding
        else:
            encoding = 'UTF-8'
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
        if hasattr(self._stream, 'encoding') and self._stream.encoding is not None:
            return self._stream.encoding
        else:
            return 'UTF-8'

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
