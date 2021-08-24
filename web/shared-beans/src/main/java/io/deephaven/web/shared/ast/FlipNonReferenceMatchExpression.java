package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

/**
 * Enforces that all IN-type expressions have a reference on the left side and only literals on the
 * right side. Any non-reference on the left or reference on the right will trigger the entire
 * IN-type expression being replaced with an OR or AND, with a sub-IN for each expression on the
 * right side.
 *
 *
 * Examples: o ColumnA in 1, 2, 3 - left as-is o ColumnA in 1, 2, ColumnB - rewritten as (ColumnA in
 * 1 OR ColumnA in 2 OR ColumnA in ColumnB) o 1 in 3, 4, 5 - will be rewritten as (3 in 1 OR 4 in 1
 * OR 5 in 1). This is a silly case, but we're not judging. At this step. o 1 in ColumnA, 4, 5 -
 * will be rewritten as (ColumnA in 1 OR 4 in 1 OR 5 in 1) o 1 in ColumnA - will be rewritten as
 * ColumnA in 1 o ColumnA in ColumnB - will be rewritten as ColumnB in ColumnA. Note that like the
 * second example, this isn't productive on its own, but as a pair with a reference on the right, it
 * will be noticed by {@link ConvertInvalidInExpressions}.
 *
 * It is assumed that some time after this step, related "in" expressions will be merged together,
 * and that these one-off expressions will get checked later.
 */
public class FlipNonReferenceMatchExpression extends ReplacingVisitor {
    public static FilterDescriptor execute(final FilterDescriptor filter) {
        return new FlipNonReferenceMatchExpression().visit(filter);
    }

    @Override
    public FilterDescriptor onIn(final FilterDescriptor descriptor) {
        return handleIn(descriptor, FilterDescriptor.FilterOperation.OR);
    }

    @Override
    public FilterDescriptor onInIgnoreCase(final FilterDescriptor descriptor) {
        return handleIn(descriptor, FilterDescriptor.FilterOperation.OR);
    }

    @Override
    public FilterDescriptor onNotIn(final FilterDescriptor descriptor) {
        return handleIn(descriptor, FilterDescriptor.FilterOperation.AND);
    }

    @Override
    public FilterDescriptor onNotInIgnoreCase(final FilterDescriptor descriptor) {
        return handleIn(descriptor, FilterDescriptor.FilterOperation.AND);
    }

    private FilterDescriptor handleIn(final FilterDescriptor descriptor,
        final FilterDescriptor.FilterOperation op) {
        // check each child - if we pass all checks we will give up
        boolean rewrite = descriptor.getChildren()[0]
            .getOperation() != FilterDescriptor.FilterOperation.REFERENCE;
        for (int i = 1; !rewrite && i < descriptor.getChildren().length; i++) {
            if (descriptor.getChildren()[i]
                .getOperation() == FilterDescriptor.FilterOperation.REFERENCE) {
                rewrite = true;
            }
        }
        if (!rewrite) {
            return descriptor;
        }
        final int count = descriptor.getChildren().length - 1;

        if (count == 1) {
            // make a single node to replace with, just swap the order of the two children
            return new FilterDescriptor(descriptor.getOperation(), null, null,
                new FilterDescriptor[] {descriptor.getChildren()[1], descriptor.getChildren()[0]});
        }

        // make a AND/OR to join each of the new children with
        final FilterDescriptor replacement = new FilterDescriptor();
        replacement.setOperation(op);
        replacement.setChildren(new FilterDescriptor[count]);
        for (int i = 0; i < count; i++) {
            replacement.getChildren()[i] =
                new FilterDescriptor(descriptor.getOperation(), null, null, new FilterDescriptor[] {
                        descriptor.getChildren()[i + 1], descriptor.getChildren()[0]});
        }

        return replacement;
    }
}
