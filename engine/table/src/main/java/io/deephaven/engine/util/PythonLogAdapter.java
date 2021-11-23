package io.deephaven.engine.util;

import java.io.PrintStream;

/**
 * This class is stored in the sys.stdout and sys.stderr variables inside of a Python session, so that we can intercept
 * the Python session's output, rather than having it all go to the system stdout/stderr streams, which are not
 * accessible to the console.
 */
class PythonLogAdapter {
    private final PrintStream out;

    private PythonLogAdapter(PrintStream out) {
        this.out = out;
    }

    /**
     * This method is used from Python so that we appear as a stream.
     *
     * We don't want to write the trailing newline, as the Logger implementation will do that for us. If there is no
     * newline; we need to remember that we added one, so that we can suppress the next empty newline. If there was a
     * newline, we shouldn't suppress it (e.g., when printing just a blank line to the output we need to preserve it).
     *
     * @param s the string to write
     * @return the number of characters written
     */
    public int write(String s) {
        out.print(s);
        return s.length();
    }

    /**
     * https://docs.python.org/2/library/io.html#io.IOBase.flush
     * https://github.com/python/cpython/blob/2.7/Modules/_io/iobase.c
     */
    public void flush() {
        out.flush();
    }

    // note: technically, we *should* be implementing the other methods present on stdout / stderr.
    // but it's *very* unlikely that anybody will be calling most of the other methods.
    //
    // Maybe these?
    // https://docs.python.org/2/library/io.html#io.RawIOBase.write
    // https://docs.python.org/2/library/io.html#io.IOBase.close

    /**
     * If we just allow python to print to it's regular STDOUT and STDERR, then it bypasses the Java System.out/err.
     *
     * We replace the stdout/stderr with a small log adapter so that console users still get their output.
     *
     * @param pythonHolder the PythonHolder object which we will insert our adapters into
     */
    static void interceptOutputStreams(PythonEvaluator pythonHolder) {
        pythonHolder.set("_stdout", new PythonLogAdapter(System.out));
        pythonHolder.set("_stderr", new PythonLogAdapter(System.err));
        pythonHolder.evalStatement("import sys");
        pythonHolder.evalStatement("sys.stdout = _stdout");
        pythonHolder.evalStatement("sys.stderr = _stderr");
    }
}
