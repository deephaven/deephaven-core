package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.FilterOperation;

/**
 * Rewrites logical expressions into an actual version that does what would be expected. Right now this is just
 * equalsIgnoreCase and its negation, to support null values.
 */
public class MakeExpressionsNullSafe extends ReplacingVisitor {
    public static FilterDescriptor execute(FilterDescriptor descriptor) {
        return new MakeExpressionsNullSafe().visit(descriptor);
    }

    @Override
    public FilterDescriptor onEqualIgnoreCase(FilterDescriptor descriptor) {
        return rewriteEqualIgnoreCaseExpression(descriptor);
    }

    @Override
    public FilterDescriptor onNotEqualIgnoreCase(FilterDescriptor descriptor) {
        return rewriteEqualIgnoreCaseExpression(descriptor).not();
    }

    private FilterDescriptor rewriteEqualIgnoreCaseExpression(FilterDescriptor descriptor) {
        // rewriting A (is-equal-ignore-case) B
        // to A == null ? B == null : A.equalsIgnoreCase(B)
        // without the ternary, this is:
        // (A == null && B == null) || (A != null && A.equalsIgnoreCase(B))
        FilterDescriptor lhs = descriptor.getChildren()[0];
        FilterDescriptor rhs = descriptor.getChildren()[1];

        return node(
                FilterOperation.OR,
                node(FilterOperation.AND,
                        node(FilterOperation.IS_NULL, lhs),
                        node(FilterOperation.IS_NULL, rhs)),
                node(FilterOperation.AND,
                        node(FilterOperation.NOT,
                                node(FilterOperation.IS_NULL, lhs)),
                        nodeEqIgnoreCase(FilterOperation.INVOKE, lhs, rhs)));
    }

    private FilterDescriptor nodeEqIgnoreCase(FilterOperation invoke, FilterDescriptor lhs, FilterDescriptor rhs) {
        FilterDescriptor node = node(invoke, lhs, rhs);
        node.setValue("equalsIgnoreCase");// note that this would fail validation
        return node;
    }

    private FilterDescriptor node(FilterOperation op, FilterDescriptor... children) {
        FilterDescriptor descriptor = new FilterDescriptor(op, null, null, children);
        return descriptor;
    }

}
