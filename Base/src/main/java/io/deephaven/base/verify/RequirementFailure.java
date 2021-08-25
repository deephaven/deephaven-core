/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.verify;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

// ----------------------------------------------------------------
/**
 * {@link RuntimeException} to be thrown on requirement failures.
 */
public class RequirementFailure extends RuntimeException {

    /**
     * The number of stack frames that should be removed from the stack to find the method whose requirements did not
     * hold.
     */
    private int m_nCallsBelowRequirer;

    // ----------------------------------------------------------------
    public RequirementFailure(String message, int nCallsBelowRequirer) {
        super(message);
        m_nCallsBelowRequirer = nCallsBelowRequirer;
    }

    // ----------------------------------------------------------------
    public RequirementFailure(String message, Exception caughtException, int nCallsBelowRequirer) {
        super(message, caughtException);
        m_nCallsBelowRequirer = nCallsBelowRequirer;
    }

    // ----------------------------------------------------------------
    /**
     * Gets the number of stack frames that should be removed from the stack to find the caller which failed to meet
     * requirements.
     */
    public int getNumCallsBelowRequirer() {
        return m_nCallsBelowRequirer;
    }

    // ----------------------------------------------------------------
    @Override
    public void printStackTrace() {
        printStackTrace(System.err);
    }

    // ----------------------------------------------------------------
    @Override
    public void printStackTrace(PrintStream s) {
        s.print(getFixedStackTrace());
    }

    // ----------------------------------------------------------------
    @Override
    public void printStackTrace(PrintWriter s) {
        s.print(getFixedStackTrace());
    }

    // ----------------------------------------------------------------
    /**
     * Gets a stack trace with a line added identifying the offending stack frame.
     */
    private StringBuffer getFixedStackTrace() {
        StringBuffer sb = getOriginalStackTrace();

        String sStackTrace = sb.toString();
        int nInsertPoint = sStackTrace.indexOf("\n\tat ");
        for (int nCount = m_nCallsBelowRequirer; nCount >= 0; nCount--) {
            nInsertPoint = sStackTrace.indexOf("\n\tat ", nInsertPoint + 1);
        }
        if (-1 != nInsertPoint) {
            sb.insert(nInsertPoint, "\n    (culprit:)");
        }
        return sb;
    }

    // ----------------------------------------------------------------
    /**
     * Gets the unmodified stack trace, instead of the one with the culprit identified.
     */
    public StringBuffer getOriginalStackTrace() {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        super.printStackTrace(printWriter);
        printWriter.close();
        return stringWriter.getBuffer();
    }

    // ----------------------------------------------------------------
    /**
     * If this stack frame caused the exception, adjust the culprit to be the caller. Used when a delegating method
     * can't verify all requirements itself but shouldn't receive the blame.
     */
    public RequirementFailure adjustForDelegatingMethod() {
        if (isThisStackFrameCulprit(1)) {
            m_nCallsBelowRequirer++;
        }
        return this;
    }

    // ----------------------------------------------------------------
    /**
     * If this stack frame caused the exception, adjust the culprit to be the caller. Used when a delegating method
     * can't verify all requirements itself but shouldn't receive the blame.
     */
    public RequirementFailure adjustForDelegatingMethodAndSyntheticAccessor() {
        if (isThisStackFrameCulprit(0)) {
            m_nCallsBelowRequirer += 2;
        }
        return this;
    }

    // ----------------------------------------------------------------
    /**
     * Returns true if this stack frame is blamed for causing the exception.
     */
    public boolean isThisStackFrameCulprit(int nFramesBelowTargetFrame) {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        StackTraceElement[] failureStackTrace = getStackTrace();
        return failureStackTrace.length - m_nCallsBelowRequirer == stackTrace.length - nFramesBelowTargetFrame;
    }
}
