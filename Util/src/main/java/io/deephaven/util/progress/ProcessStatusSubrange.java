package io.deephaven.util.progress;

import io.deephaven.base.verify.Require;

import java.util.function.Supplier;

/**
 * An interface class that allows a StatusCallback to represent a portion of another's range.
 */
public class ProcessStatusSubrange implements StatusCallback {
    private final int begin;
    private final int end;
    private final StatusCallback parent;
    private int stepProgress;

    public ProcessStatusSubrange(StatusCallback parentCallback, int begin, int end) {
        Require.geqZero(begin, "begin");
        Require.leq(end, "end", 100);
        Require.leq(begin, "begin", end, "end");

        this.begin = begin;
        this.end = end;

        this.parent = parentCallback;
    }

    /**
     * This object has been assigned a range of the parents range. Adjust the stepProgress value from 0-100 to the value
     * in the full range of the parent object.
     * 
     * @param progress percent complete (0-100)
     * @param status optional message text
     */
    @Override
    public void update(final int progress, final Supplier<String> status) {
        Require.geqZero(progress, "progress");
        Require.leq(progress, "progress", 100);
        // Require.geq(getStepValue(), "current progress", progress, "progress"); // Needed? probably not.

        this.stepProgress = progress;
        int adjustedProgress = (int) ((progress / 100.0) * (end - begin) + begin);
        parent.update(adjustedProgress, status);
    }

    @Override
    public int getValue() {
        return parent.getValue();
    }

    @Override
    public int getStepValue() {
        return stepProgress;
    }
}
