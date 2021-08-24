package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

/**
 * Rewrites NOT expressions, with the goal of removing as many as possible and simplifying the
 * expression tree. This visitor delegates its work to {@link FilterDescriptor#not()}, which uses
 * the following rules:
 * <ul>
 * <li>Distribute NOTs to children of AND/OR expressions, via DeMorgan's law.</li>
 * <li>{@code NOT(NOT(A))} is replaced with A.</li>
 * <li>Replace any operation with its opposite, if any. For example, {@code NOT(A >= B)} is replaced
 * with {@code A < B}, and likewise for all the other inequality operators, {@code EQ}, and
 * {@code IN}.</li>
 * <li>Other operations {@code IS_NULL}, {@code INVOKE}, {@code SEARCH}, {@code CONTAINS} are left
 * as-is, wrapped wrapped with a {@code NOT}.</li>
 * </ul>
 */
public class NormalizeNots extends ReplacingVisitor {
    public static FilterDescriptor execute(FilterDescriptor descriptor) {
        return new NormalizeNots().visit(descriptor);
    }

    @Override
    public FilterDescriptor onNot(FilterDescriptor descriptor) {
        // First deal with this not, then visit its children. This way when we remove the current
        // node, we have
        // the change to remove children too, instead of rewriting other nodes twice.
        return visitChildren(descriptor.getChildren()[0].not());
    }
}
