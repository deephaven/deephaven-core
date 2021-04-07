package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.List;
import java.util.function.UnaryOperator;

public class FilterPrinter implements FilterVisitor {
    private final StringBuilder sb = new StringBuilder();
    private final UnaryOperator<String> stringEscape;

    public FilterPrinter(UnaryOperator<String> stringEscape) {
        this.stringEscape = stringEscape;
    }

    public String print(Condition condition) {
        assert sb.length() == 0 : "sb.length() == 0";
        FilterVisitor.accept(condition, this);

        return sb.toString();
    }

    @Override
    public void onAnd(List<Condition> filtersList) {
        if (filtersList.isEmpty()) {
            return;//should be pruned earlier
        }
        if (filtersList.size() == 1) {
            FilterVisitor.accept(filtersList.get(0), this);//should have been stripped earlier
            return;
        }
        sb.append("(");

        // at least 2 entries, handle the first, then the rest
        FilterVisitor.accept(filtersList.get(0), this);//should have been stripped earlier
        filtersList.stream().skip(1).forEach(condition -> {
            sb.append(" && ");
            FilterVisitor.accept(condition, this);
        });

        sb.append(")");
    }

    @Override
    public void onOr(List<Condition> filtersList) {
        if (filtersList.isEmpty()) {
            return;//should be pruned earlier
        }
        if (filtersList.size() == 1) {
            FilterVisitor.accept(filtersList.get(0), this);//should have been stripped earlier
            return;
        }
        sb.append("(");

        // at least 2 entries, handle the first, then the rest
        FilterVisitor.accept(filtersList.get(0), this);//should have been stripped earlier
        filtersList.stream().skip(1).forEach(condition -> {
            sb.append(" || ");
            FilterVisitor.accept(condition, this);
        });

        sb.append(")");
    }

    @Override
    public void onNot(Condition filter) {
        sb.append("!(");
        FilterVisitor.accept(filter, this);
        sb.append(")");
    }

    @Override
    public void onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
        accept(lhs);
        switch (operation) {
            case LESS_THAN:
                sb.append(" < ");
                break;
            case LESS_THAN_OR_EQUAL:
                sb.append(" <= ");
                break;
            case GREATER_THAN:
                sb.append(" > ");
                break;
            case GREATER_THAN_OR_EQUAL:
                sb.append(" >= ");
                break;
            case EQUALS:
                sb.append(" == ");
                break;
            case NOT_EQUALS:
                sb.append(" != ");
                break;
            case UNRECOGNIZED:
            default:
                throw new UnsupportedOperationException("Unknown operation " + operation);
        }
        accept(rhs);
    }

    @Override
    public void onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
        if (candidatesList.isEmpty()) {
            return;// should have already been pruned
        }
        accept(target);
        if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
            sb.append(" icase");
        }
        if (matchType == MatchType.INVERTED) {
            sb.append(" not");
        }
        sb.append(" in ");
        accept(candidatesList.get(0));
        for (int i = 1; i < candidatesList.size(); i++) {
            sb.append(", ");
            accept(candidatesList.get(i));
        }
    }

    @Override
    public void onIsNull(Reference reference) {
        sb.append("isNull(");
        onReference(reference);
        sb.append(")");
    }

    @Override
    public void onInvoke(String method, Value target, List<Value> argumentsList) {
        if (target != null) {
            accept(target);
            sb.append(".");
        }
        sb.append(method).append("(");
        for (int i = 0; i < argumentsList.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            accept(argumentsList.get(i));
        }
        sb.append(")");

    }

    @Override
    public void onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType) {
        if (matchType == MatchType.INVERTED) {
            sb.append("!");
        }
        sb.append("contains");
        if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
            sb.append("IgnoreCase");
        }
        sb.append("(");
        onReference(reference);
        sb.append(",");
        sb.append(stringEscape.apply(searchString));
        sb.append(")");
    }

    @Override
    public void onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {
        if (matchType == MatchType.INVERTED) {
            sb.append("!");
        }
        sb.append("matches");
        if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
            sb.append("IgnoreCase");
        }
        sb.append("(");
        onReference(reference);
        sb.append(",");
        sb.append(stringEscape.apply(regex));
        sb.append(")");
    }

    @Override
    public void onSearch(String searchString, List<Reference> optionalReferencesList) {
        sb.append("searchTableColumns(");
        sb.append(stringEscape.apply(searchString));
        for (Reference reference : optionalReferencesList) {
            sb.append(",");
            onReference(reference);
        }
        sb.append(")");
    }

    private void accept(Value value) {
        switch (value.getDataCase()) {
            case REFERENCE:
                onReference(value.getReference());
                break;
            case LITERAL:
                onLiteral(value.getLiteral());
                break;
            case DATA_NOT_SET:
            default:
                throw new UnsupportedOperationException("Unknown value " + value);
        }
    }

    private void onReference(Reference reference) {
        sb.append(reference.getColumnName());
    }

    private void onLiteral(Literal literal) {
        switch (literal.getValueCase()) {
            case STRINGVALUE:
                sb.append(stringEscape.apply(literal.getStringValue()));
                break;
            case DOUBLEVALUE:
                sb.append(literal.getDoubleValue());
                break;
            case BOOLVALUE:
                sb.append(literal.getBoolValue());
                break;
            case LONGVALUE:
                sb.append(literal.getLongValue());
                break;
            case NANOTIMEVALUE:
                sb.append("new DBDateTime(").append(literal.getNanoTimeValue()).append(")");
                break;
            case VALUE_NOT_SET:
            default:
                throw new UnsupportedOperationException("Unknown literal " + literal);
        }
    }
}
