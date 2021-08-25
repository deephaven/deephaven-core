package io.deephaven.lang.completion;

/**
 * A Deephaven-internal completion fragment.
 *
 * Represents a source code replacement option, with fields for "what code to insert", "where to
 * insert it", and "what to render for completion, if different from code insertion".
 */
public class CompletionFragment {
    int start; // where to start the replacement
    int length; // where to end the replacement
    String completion; // what to replace with
    String displayCompletion; // what to replace with

    public CompletionFragment(int start, int length, String completion) {
        this.start = start;
        this.length = length;
        this.completion = completion;
        this.displayCompletion = completion;
    }

    public CompletionFragment(int start, int length, String completion, String displayCompletion) {
        this.start = start;
        this.length = length;
        this.completion = completion;
        this.displayCompletion = displayCompletion;
    }

    public int getStart() {
        return start;
    }

    public int getLength() {
        return length;
    }

    public String getCompletion() {
        return completion;
    }

    public String getDisplayCompletion() {
        return displayCompletion;
    }

    @Override
    public String toString() {
        return "CompletionFragment{" +
            "start=" + start +
            ", length=" + length +
            ", completion='" + completion + '\'' +
            ", displayCompletion='" + displayCompletion + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final CompletionFragment that = (CompletionFragment) o;

        if (start != that.start)
            return false;
        if (length != that.length)
            return false;
        if (!completion.equals(that.completion))
            return false;
        return displayCompletion.equals(that.displayCompletion);
    }

    @Override
    public int hashCode() {
        int result = start;
        result = 31 * result + length;
        result = 31 * result + completion.hashCode();
        result = 31 * result + displayCompletion.hashCode();
        return result;
    }
}
