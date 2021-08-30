package io.deephaven.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;

/**
 * An exception to use when a series of operations must all be executed, but may all throw
 * exceptions themselves. This allows for retention of all exception data.
 */
public class MultiException extends Exception {

    public static final Throwable[] ZERO_LENGTH_THROWABLE_ARRAY = new Throwable[0];
    private final Throwable[] causes;

    /**
     * Create a MultiException from an array of Throwable causes.
     *
     * @param description the message to use
     * @param causes a list of causes
     */
    public MultiException(String description, Throwable... causes) {
        super(description, getFirstCause(causes));
        this.causes = causes == null ? ZERO_LENGTH_THROWABLE_ARRAY : causes;
    }

    /**
     * If there is a single exception, return that exception; otherwise wrap the causes into a
     * MultiException.
     *
     * @param description the description for the MultiException
     * @param causes the array of causes
     * @return a MultiException or the single Throwable
     */
    public static Throwable maybeWrapInMultiException(String description, Throwable... causes) {
        if (causes.length == 1) {
            return causes[0];
        }
        return new MultiException(description, causes);
    }

    /**
     * If there is a single exception, return that exception; otherwise wrap the causes into a
     * MultiException.
     *
     * @param description the description for the MultiException
     * @param causes the list of causes
     * @return a MultiException or the single Throwable
     */
    public static Throwable maybeWrapInMultiException(String description, List<Throwable> causes) {
        if (causes.size() == 1) {
            return causes.get(0);
        }
        return new MultiException(description, causes.toArray(ZERO_LENGTH_THROWABLE_ARRAY));
    }

    /**
     * If there is a single exception, return that exception; otherwise wrap the causes into a
     * MultiException.
     *
     * @param description the description for the MultiException
     * @param causes the array of causes
     * @return a MultiException or the single Exception
     */
    public static Exception maybeWrapInMultiException(String description, Exception... causes) {
        if (causes.length == 1) {
            return causes[0];
        }
        return new MultiException(description, causes);
    }

    private static Throwable getFirstCause(Throwable[] causes) {
        if (causes == null || causes.length == 0) {
            return null;
        }

        return causes[0];
    }

    /**
     * @return all of the exceptions that resulted in this one.
     */
    public Throwable[] getCauses() {
        return causes;
    }

    @Override
    public void printStackTrace(PrintWriter s) {
        super.printStackTrace(s);
        for (int i = 0; i < causes.length; i++) {
            s.println("Cause " + (i + 1) + ":");
            causes[i].printStackTrace(s);
        }
    }

    @Override
    public void printStackTrace(PrintStream s) {
        super.printStackTrace(s);
        for (int i = 0; i < causes.length; i++) {
            s.println("Cause " + (i + 1) + ":");
            causes[i].printStackTrace(s);
        }
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getMessage()).append(": ");
        for (int i = 0; i < causes.length; i++) {
            sb.append("Cause ").append(i).append(": ");
            sb.append(causes[i].toString());
            sb.append('\n');
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
