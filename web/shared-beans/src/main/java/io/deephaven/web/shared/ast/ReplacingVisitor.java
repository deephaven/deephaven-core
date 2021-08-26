package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

public abstract class ReplacingVisitor {

    public FilterDescriptor onAnd(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onOr(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onNot(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onLessThan(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onGreaterThan(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onLessThanOrEqualTo(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onGreaterThanOrEqualTo(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onEqual(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onEqualIgnoreCase(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onNotEqual(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onNotEqualIgnoreCase(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onIn(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onInIgnoreCase(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onNotIn(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onNotInIgnoreCase(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onIsNull(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onInvoke(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onLiteral(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onReference(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onContains(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onContainsIgnoreCase(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onPattern(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onPatternIgnoreCase(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    public FilterDescriptor onSearch(FilterDescriptor descriptor) {
        return visitChildren(descriptor);
    }

    protected FilterDescriptor visit(FilterDescriptor descriptor) {
        switch (descriptor.getOperation()) {
            case AND:
                return onAnd(descriptor);
            case OR:
                return onOr(descriptor);
            case NOT:
                return onNot(descriptor);
            case LT:
                return onLessThan(descriptor);
            case GT:
                return onGreaterThan(descriptor);
            case LTE:
                return onLessThanOrEqualTo(descriptor);
            case GTE:
                return onGreaterThanOrEqualTo(descriptor);
            case EQ:
                return onEqual(descriptor);
            case EQ_ICASE:
                return onEqualIgnoreCase(descriptor);
            case NEQ:
                return onNotEqual(descriptor);
            case NEQ_ICASE:
                return onNotEqualIgnoreCase(descriptor);
            case IN:
                return onIn(descriptor);
            case IN_ICASE:
                return onInIgnoreCase(descriptor);
            case NOT_IN:
                return onNotIn(descriptor);
            case NOT_IN_ICASE:
                return onNotInIgnoreCase(descriptor);
            case IS_NULL:
                return onIsNull(descriptor);
            case INVOKE:
                return onInvoke(descriptor);
            case LITERAL:
                return onLiteral(descriptor);
            case REFERENCE:
                return onReference(descriptor);
            case CONTAINS:
                return onContains(descriptor);
            case CONTAINS_ICASE:
                return onContainsIgnoreCase(descriptor);
            case MATCHES:
                return onPattern(descriptor);
            case MATCHES_ICASE:
                return onPatternIgnoreCase(descriptor);
            case SEARCH:
                return onSearch(descriptor);
        }
        throw new IllegalStateException("Unknown operation " + descriptor.getOperation());
    }

    protected FilterDescriptor visitChildren(FilterDescriptor descriptor) {
        boolean changed = false;

        FilterDescriptor[] children = new FilterDescriptor[descriptor.getChildren().length];
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            if (descriptor.getOperation() == FilterDescriptor.FilterOperation.INVOKE && i == 0) {
                if (descriptor.getChildren()[i] == null) {
                    continue;// first child in an invoke may be null, if so, skip it
                }
            }

            children[i] = visit(descriptor.getChildren()[i]);
            if (children[i] != descriptor.getChildren()[i]) {
                changed = true;
            }
        }

        if (!changed) {
            return descriptor;
        }
        return new FilterDescriptor(descriptor.getOperation(), descriptor.getValue(), descriptor.getType(), children);
    }

}
