package io.deephaven.base.system;

import java.io.PrintStream;
import java.util.Objects;

/**
 * Captures {@link System#out} and {@link System#err}. It is primarily useful for logging infrastructure where the
 * output streams may have been redirected.
 */
public class PrintStreamGlobals {

    private static final PrintStream OUT;
    private static final PrintStream ERR;

    static {
        OUT = Objects.requireNonNull(System.out, "System.out");
        ERR = Objects.requireNonNull(System.err, "System.err");
    }

    /**
     * Use this method to force this class and its statics to be initialized. Should be used before an application is
     * re-directing stdout / stderr if it wants to have global access to the original streams. While the other methods
     * in this class could be used for initialization, this method provides the appropriate context, and should be used
     * instead.
     */
    public static void init() {
        // empty on purpose
    }

    /**
     * @return {@link System#out}, as seen at class initialization time
     */
    public static PrintStream getOut() {
        return OUT;
    }

    /**
     * @return {@link System#err}, as seen at class initialization time
     */
    public static PrintStream getErr() {
        return ERR;
    }
}
