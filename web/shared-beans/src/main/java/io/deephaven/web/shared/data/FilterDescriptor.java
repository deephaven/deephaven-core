package io.deephaven.web.shared.data;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;

public class FilterDescriptor implements Serializable {

    private static FilterDescriptor[] EMPTY = new FilterDescriptor[0];

    public enum Kind {
        Condition, Value
    }
    public enum FilterOperation {
        // 2+ children are conditions
        AND(Kind.Condition), OR(Kind.Condition),
        // 1 child, condition. this is sugar over almost any other Condition operation
        NOT(Kind.Condition),
        // 2 children are values
        LT(Kind.Condition), GT(Kind.Condition), LTE(Kind.Condition), GTE(Kind.Condition), EQ(
            Kind.Condition), EQ_ICASE(
                Kind.Condition), NEQ(Kind.Condition), NEQ_ICASE(Kind.Condition),

        // 2+ children are values
        IN(Kind.Condition), IN_ICASE(Kind.Condition), NOT_IN(Kind.Condition), NOT_IN_ICASE(
            Kind.Condition),
        // 1 child is anything (probably just value)
        IS_NULL(Kind.Condition),
        // 0+ children are anything
        INVOKE(Kind.Condition),
        // 0 children
        LITERAL(Kind.Value), REFERENCE(Kind.Value),

        CONTAINS(Kind.Condition), CONTAINS_ICASE(Kind.Condition), MATCHES(
            Kind.Condition), MATCHES_ICASE(Kind.Condition),

        SEARCH(Kind.Condition),
        ;

        public final Kind expressionKind;

        FilterOperation(Kind expressionKind) {
            this.expressionKind = expressionKind;
        }
    }

    /**
     * Describes types of value literals. This is much rougher than we'll eventually want, but as an
     * internal-only detail it does fit our purposes
     */
    public enum ValueType {
        // js/java String
        String,
        // js/java Number/Double
        Number,
        // js/java Boolean
        Boolean,
        // wrapped DateWrapper, string will be a long
        Datetime,
        // gwt-emulated long, usually in the form of a LongWrapper
        Long,
        // TODO handle other cases?
        Other
    }

    // the operation performed by this node, non-null
    private FilterOperation operation;
    // used for invoke, literal, reference, null otherwise
    private String value;
    // type of the literal value if not null - for non-literals, will be null
    private ValueType type;
    // passed to the various operations - in the case of static invoke, first will be null.
    // The array itself is non-null
    private FilterDescriptor[] children;

    public FilterDescriptor() {
        children = EMPTY;
    }

    public FilterDescriptor(FilterOperation operation, @Nullable String value,
        @Nullable ValueType type, FilterDescriptor[] children) {
        setOperation(operation);
        setValue(value);
        setType(type);
        setChildren(children);
    }

    public FilterOperation getOperation() {
        return operation;
    }

    public void setOperation(FilterOperation operation) {
        this.operation = operation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ValueType getType() {
        return type;
    }

    public void setType(ValueType type) {
        this.type = type;
    }

    public FilterDescriptor[] getChildren() {
        return children;
    }

    public void setChildren(FilterDescriptor[] children) {
        this.children = children;
    }

    public void accept(Visitor visitor) {
        switch (operation) {
            case AND:
                visitor.onAnd(this);
                break;
            case OR:
                visitor.onOr(this);
                break;
            case NOT:
                visitor.onNot(this);
                break;
            case LT:
                visitor.onLessThan(this);
                break;
            case GT:
                visitor.onGreaterThan(this);
                break;
            case LTE:
                visitor.onLessThanOrEqualTo(this);
                break;
            case GTE:
                visitor.onGreaterThanOrEqualTo(this);
                break;
            case EQ:
                visitor.onEqual(this);
                break;
            case EQ_ICASE:
                visitor.onEqualIgnoreCase(this);
                break;
            case NEQ:
                visitor.onNotEqual(this);
                break;
            case NEQ_ICASE:
                visitor.onNotEqualIgnoreCase(this);
                break;
            case IN:
                visitor.onIn(this);
                break;
            case IN_ICASE:
                visitor.onInIgnoreCase(this);
                break;
            case NOT_IN:
                visitor.onNotIn(this);
                break;
            case NOT_IN_ICASE:
                visitor.onNotInIgnoreCase(this);
                break;
            case IS_NULL:
                visitor.onIsNull(this);
                break;
            case INVOKE:
                visitor.onInvoke(this);
                break;
            case LITERAL:
                visitor.onLiteral(this);
                break;
            case REFERENCE:
                visitor.onReference(this);
                break;
            case CONTAINS:
                visitor.onContains(this);
                break;
            case CONTAINS_ICASE:
                visitor.onContainsIgnoreCase(this);
                break;
            case MATCHES:
                visitor.onPattern(this);
                break;
            case MATCHES_ICASE:
                visitor.onPatternIgnoreCase(this);
                break;
            case SEARCH:
                visitor.onSearch(this);
                break;
            default:
                throw new IllegalStateException("Unhandled case " + operation);
        }

    }

    public interface Visitor {
        void onAnd(FilterDescriptor descriptor);

        void onOr(FilterDescriptor descriptor);

        void onNot(FilterDescriptor descriptor);

        void onLessThan(FilterDescriptor descriptor);

        void onGreaterThan(FilterDescriptor descriptor);

        void onLessThanOrEqualTo(FilterDescriptor descriptor);

        void onGreaterThanOrEqualTo(FilterDescriptor descriptor);

        void onEqual(FilterDescriptor descriptor);

        void onEqualIgnoreCase(FilterDescriptor descriptor);

        void onNotEqual(FilterDescriptor descriptor);

        void onNotEqualIgnoreCase(FilterDescriptor descriptor);

        void onIn(FilterDescriptor descriptor);

        void onInIgnoreCase(FilterDescriptor descriptor);

        void onNotIn(FilterDescriptor descriptor);

        void onNotInIgnoreCase(FilterDescriptor descriptor);

        void onIsNull(FilterDescriptor descriptor);

        void onInvoke(FilterDescriptor descriptor);

        void onLiteral(FilterDescriptor descriptor);

        void onReference(FilterDescriptor descriptor);

        void onContains(FilterDescriptor descriptor);

        void onContainsIgnoreCase(FilterDescriptor descriptor);

        void onPattern(FilterDescriptor descriptor);

        void onPatternIgnoreCase(FilterDescriptor descriptor);

        void onSearch(FilterDescriptor descriptor);
    }

    // TODO probably move this out to an optimization, or to the api itself?
    public FilterDescriptor not() {
        assert getOperation().expressionKind == Kind.Condition;
        FilterDescriptor negative = new FilterDescriptor();

        // general case is that we reference the same list of children, except in three situations:
        // * AND/OR we switch to the other, and NOT all children (demorgans law)
        // * NOT we remove the other NOT
        // * INVOKE/IS_NULL we just wrap in NOT, interpret it at runtime
        negative.setChildren(getChildren());

        switch (getOperation()) {
            case AND:
                // demorgan means we need to also not() the children...
                negative.setOperation(FilterOperation.OR);
                for (int i = 0; i < negative.getChildren().length; i++) {
                    negative.getChildren()[i] = negative.getChildren()[i].not();
                }
                break;
            case OR:
                // demorgan means we need to also not() the children...
                negative.setOperation(FilterOperation.AND);
                for (int i = 0; i < negative.getChildren().length; i++) {
                    negative.getChildren()[i] = negative.getChildren()[i].not();
                }
                break;
            case NOT:
                // unwrap child, return that instead of a new item
                assert getChildren().length == 1;
                return getChildren()[0];
            case LT:
                negative.setOperation(FilterOperation.GTE);
                break;
            case GT:
                negative.setOperation(FilterOperation.LTE);
                break;
            case LTE:
                negative.setOperation(FilterOperation.GT);
                break;
            case GTE:
                negative.setOperation(FilterOperation.LT);
                break;
            case EQ:
                negative.setOperation(FilterOperation.NEQ);
                break;
            case EQ_ICASE:
                negative.setOperation(FilterOperation.NEQ_ICASE);
                break;
            case NEQ:
                negative.setOperation(FilterOperation.EQ);
                break;
            case NEQ_ICASE:
                negative.setOperation(FilterOperation.EQ_ICASE);
                break;
            case IN:
                negative.setOperation(FilterOperation.NOT_IN);
                break;
            case IN_ICASE:
                negative.setOperation(FilterOperation.NOT_IN_ICASE);
                break;
            case NOT_IN:
                negative.setOperation(FilterOperation.IN);
                break;
            case NOT_IN_ICASE:
                negative.setOperation(FilterOperation.IN_ICASE);
                break;
            case IS_NULL:
            case INVOKE:
            case CONTAINS:
            case CONTAINS_ICASE:
            case MATCHES:
            case MATCHES_ICASE:
                // special case, we simply wrap these in the NOT operation
                negative.setOperation(FilterOperation.NOT);
                negative.setChildren(new FilterDescriptor[] {this});
                break;
            case SEARCH:
                throw new IllegalStateException("Cannot not() a search");
            case LITERAL:
            case REFERENCE:
                throw new IllegalStateException("Cannot not() a literal or reference");
            default:
                throw new IllegalArgumentException("Unhandled operation: " + getOperation());
        }

        return negative;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final FilterDescriptor that = (FilterDescriptor) o;

        if (operation != that.operation)
            return false;
        if (value != null ? !value.equals(that.value) : that.value != null)
            return false;
        if (type != that.type)
            return false;
        return Arrays.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        int result = operation.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(children);
        return result;
    }

    @Override
    public String toString() {
        return "FilterDescriptor{" +
            "operation=" + operation +
            ", value='" + value + '\'' +
            ", type=" + type +
            ", children=" + Arrays.toString(children) +
            '}';
    }
}
