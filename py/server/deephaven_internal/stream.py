import io


class TeeStream(io.TextIOBase):
    """
    TextIOBase subclass that splits output between a delegate instance and a set of lambdas. Can delegate some calls to
    the provided stream but actually write only to the lambdas.

    Useful to adapt any existing file-like object (such as sys.stdout or sys.stderr), but actually let some other
    processing take place before writing.
    """

    encoding = 'UTF-8'
    closed = False

    @staticmethod
    def split(py_stream, java_stream):
        return TeeStream(
            py_stream,
            True,
            lambda t: java_stream.write(bytes(t, py_stream.encoding)),
            lambda: java_stream.flush(),
            lambda: java_stream.close()
        )

    @staticmethod
    def redirect(py_stream, java_stream):
        if hasattr(py_stream, "encoding"):
            encoding = py_stream.encoding
        else:
            encoding = 'UTF-8'
        return TeeStream(
            py_stream,
            True,
            lambda t: java_stream.write(bytes(t, encoding)),
            lambda: java_stream.flush(),
            lambda: java_stream.close()
        )

    def __init__(self, orig_stream, should_write_to_orig_stream, write_func, flush_func, close_func):
        """
        Creates a new TeeStream to let output be written from out place, but be sent to
        multiple places.

        Ideally, the stream would be passed as more funcs, but we have to manage correctly
        propagating certain non-java details like encoding and isatty.
        :param orig_stream: the underlying python stream to use as a prototype
        :param should_write_to_orig_stream: True to delegate writes to the original stream, False to only write to the
        lambdas that follow
        :param write_func: a function to call when data should be written
        :param flush_func: a function to call when data should be flushed
        :param close_func: a function to call when the stream should be closed
        """
        self._stream = orig_stream
        self.should_write_to_orig_stream = should_write_to_orig_stream
        self.write_func = write_func
        self.flush_func = flush_func
        self.close_func = close_func

        if hasattr(orig_stream, 'encoding') and orig_stream.encoding is not None:
            # Read this in the constructor, since encoding is read as an attr, not a method
            self.encoding = orig_stream.encoding

    def isatty(self):
        if not hasattr(self._stream, "isatty"):
            return False
        return self._stream.isatty()

    def fileno(self):
        if not hasattr(self._stream, "fileno"):
            return None
        return self._stream.fileno()

    def write(self, string):
        self.write_func(string)
        if self.should_write_to_orig_stream:
            self._stream.write(string)

    def flush(self):
        self.flush_func()
        if self.should_write_to_orig_stream:
            self._stream.flush()

    def close(self):
        self.close_func()
        self.closed = True
        if self.should_write_to_orig_stream:
            self._stream.close()

