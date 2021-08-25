package io.deephaven.db.exceptions;

/**
 * An unchecked exception indicating a problem with a parsed expression, for example in
 * {@link io.deephaven.db.tables.Table#where(String...)} or {@link io.deephaven.db.tables.Table#update(String...)}.
 */
public class ExpressionException extends UncheckedTableException {
    final String problemExpression;

    public ExpressionException(String reason, String problemExpression) {
        super(reason);
        this.problemExpression = problemExpression;
    }

    public ExpressionException(String reason, Throwable cause, String problemExpression) {
        super(reason, cause);
        this.problemExpression = problemExpression;
    }

    public ExpressionException(Throwable cause, String problemExpression) {
        super(cause);
        this.problemExpression = problemExpression;
    }

    /**
     * Get the expression that has a problem.
     *
     * @return the problem expression
     */
    @SuppressWarnings("unused")
    public final String getProblemExpression() {
        return problemExpression;
    }
}
