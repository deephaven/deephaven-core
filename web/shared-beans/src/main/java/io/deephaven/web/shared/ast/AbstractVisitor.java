package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

public class AbstractVisitor implements FilterDescriptor.Visitor {
    protected void visitChildren(FilterDescriptor[] children) {
        for (FilterDescriptor child : children) {
            child.accept(this);
        }
    }

    protected void visitInvokeChildren(FilterDescriptor[] children) {
        if (children.length > 0 && children[0] != null) {
            children[0].accept(this);
        }
        for (int i = 1; i < children.length; i++) {
            children[i].accept(this);
        }
    }

    @Override
    public void onAnd(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onOr(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onNot(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onLessThan(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onGreaterThan(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onLessThanOrEqualTo(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onGreaterThanOrEqualTo(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onEqual(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onEqualIgnoreCase(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onNotEqual(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onNotEqualIgnoreCase(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onIn(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onInIgnoreCase(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onNotIn(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onNotInIgnoreCase(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onIsNull(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onInvoke(FilterDescriptor descriptor) {
        visitInvokeChildren(descriptor.getChildren());
    }

    @Override
    public void onLiteral(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onReference(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onContains(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onContainsIgnoreCase(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onPattern(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onPatternIgnoreCase(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }

    @Override
    public void onSearch(FilterDescriptor descriptor) {
        visitChildren(descriptor.getChildren());
    }
}
