package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

/**
 * Rewrite any IN-type expression into its corresponding EQUALS if the left side is not a reference or if the right side
 * does have a reference. Assumes that FlipNonReferenceMatchExpression has already been run, making this the second
 * attempt to deal with these, and letting us be confident that these expressions cannot be expressed as more efficient
 * "in"s.
 *
 * Examples: o ColumnA in 1 - left as is o ColumnA in 1, 2 - left as is o 1 in 2 - rewritten to 1 == 2. o ColumnA in
 * ColumnB - rewritten to ColumnA == ColumnB
 *
 * Signs that visitors were mis-ordered: o 1 in ColumnA - literal on LHS should already be handled o 1 in 2, 3 - literal
 * on LHS with multiple RHS values should already be handled, should have been flipped and split into individual exprs o
 * ColumnA in ColumnB, 2 - column ref on RHS should already be handled
 */
public class ConvertInvalidInExpressions extends ReplacingVisitor {
    public static FilterDescriptor execute(FilterDescriptor filter) {
        return new ConvertInvalidInExpressions().visit(filter);
    }

    @Override
    public FilterDescriptor onIn(FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.EQ);
    }

    private FilterDescriptor handle(FilterDescriptor descriptor,
            FilterDescriptor.FilterOperation replacementOperation) {
        if (descriptor.getChildren().length != 2
                || descriptor.getChildren()[0].getOperation() != descriptor.getChildren()[1].getOperation()) {
            return descriptor;
        }

        return new FilterDescriptor(replacementOperation, null, null, descriptor.getChildren());
    }

    @Override
    public FilterDescriptor onInIgnoreCase(FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.EQ_ICASE);
    }

    @Override
    public FilterDescriptor onNotIn(FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.NEQ);
    }

    @Override
    public FilterDescriptor onNotInIgnoreCase(FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.NEQ_ICASE);
    }
}
