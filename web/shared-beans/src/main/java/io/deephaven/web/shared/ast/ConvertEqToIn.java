package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

/**
 * IN-type expressions are more efficient at runtime than EQUALS, and also compile more quickly.
 * However, they only work when comparing a column reference to a literal value, so this
 * transformation looks for those and replaces the EQ-type node with its corresponding IN-type node.
 *
 * This is sort of the opposite of {@link ConvertInvalidInExpressions}, which converts EQs to INs
 * when the types of the children are not appropriate for use in a MatchFilter.
 *
 * Has no apparent pre-requisites.
 */
public class ConvertEqToIn extends ReplacingVisitor {
    public static FilterDescriptor execute(final FilterDescriptor descriptor) {
        return new ConvertEqToIn().visit(descriptor);
    }

    @Override
    public FilterDescriptor onEqual(final FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.IN);
    }

    @Override
    public FilterDescriptor onEqualIgnoreCase(final FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.IN_ICASE);
    }

    @Override
    public FilterDescriptor onNotEqual(final FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.NOT_IN);
    }

    @Override
    public FilterDescriptor onNotEqualIgnoreCase(final FilterDescriptor descriptor) {
        return handle(descriptor, FilterDescriptor.FilterOperation.NOT_IN_ICASE);
    }

    private FilterDescriptor handle(final FilterDescriptor descriptor,
        final FilterDescriptor.FilterOperation in) {
        // if one is a reference and one is a literal, we can process it and ensure the reference is
        // first
        final FilterDescriptor first = descriptor.getChildren()[0];
        final FilterDescriptor second = descriptor.getChildren()[1];
        if (first.getOperation() == FilterDescriptor.FilterOperation.REFERENCE
            && second.getOperation() == FilterDescriptor.FilterOperation.LITERAL) {
            return replaceWithIn(in, first, second);
        } else if (first.getOperation() == FilterDescriptor.FilterOperation.LITERAL
            && second.getOperation() == FilterDescriptor.FilterOperation.REFERENCE) {
            return replaceWithIn(in, second, first);
        } else {
            return descriptor;
        }
    }

    private FilterDescriptor replaceWithIn(final FilterDescriptor.FilterOperation operation,
        final FilterDescriptor reference, final FilterDescriptor literal) {
        return new FilterDescriptor(operation, null, null,
            new FilterDescriptor[] {reference, literal});
    }
}
