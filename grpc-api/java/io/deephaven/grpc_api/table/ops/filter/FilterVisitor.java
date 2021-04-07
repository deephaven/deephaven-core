package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.List;

public interface FilterVisitor {
    void onAnd(List<Condition> filtersList);
    void onOr(List<Condition> filtersList);
    void onNot(Condition filter);

    void onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs);

    void onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType);

    void onIsNull(Reference reference);

    void onInvoke(String method, Value target, List<Value> argumentsList);

    void onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType);
    void onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType);

    void onSearch(String searchString, List<Reference> optionalReferencesList);

    static void accept(Condition condition, FilterVisitor visitor) {
        switch (condition.getDataCase()) {
            case AND:
                visitor.onAnd(condition.getAnd().getFiltersList());
                break;
            case OR:
                visitor.onOr(condition.getAnd().getFiltersList());
                break;
            case NOT:
                visitor.onNot(condition.getNot().getFilter());
                break;
            case COMPARE:
                visitor.onComparison(condition.getCompare().getOperation(), condition.getCompare().getLhs(), condition.getCompare().getRhs());
                break;
            case IN:
                visitor.onIn(condition.getIn().getTarget(), condition.getIn().getCandidatesList(), condition.getIn().getCaseSensitivity(), condition.getIn().getMatchType());
                break;
            case INVOKE:
                visitor.onInvoke(condition.getInvoke().getMethod(), condition.getInvoke().getTarget(), condition.getInvoke().getArgumentsList());
                break;
            case ISNULL:
                visitor.onIsNull(condition.getIsNull().getReference());
                break;
            case MATCHES:
                visitor.onMatches(condition.getMatches().getReference(), condition.getMatches().getRegex(), condition.getMatches().getCaseSensitivity(), condition.getMatches().getMatchType());
                break;
            case CONTAINS:
                visitor.onContains(condition.getContains().getReference(), condition.getContains().getSearchString(), condition.getContains().getCaseSensitivity(), condition.getContains().getMatchType());
                break;
            case SEARCH:
                visitor.onSearch(condition.getSearch().getSearchString(), condition.getSearch().getOptionalReferencesList());
                break;
            case DATA_NOT_SET:
            default:
                throw new IllegalStateException("Unsupported condition " + condition);
        }
    }

    abstract class AbstractFilterVisitor implements FilterVisitor {
        protected void visitChildren(List<Condition> filtersList) {
            for (Condition condition : filtersList) {
                accept(condition, this);
            }
        }
        @Override
        public void onAnd(List<Condition> filtersList) {
            visitChildren(filtersList);
        }

        @Override
        public void onOr(List<Condition> filtersList) {
            visitChildren(filtersList);
        }

        @Override
        public void onNot(Condition filter) {
            FilterVisitor.accept(filter, this);
        }

        @Override
        public void onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {

        }

        @Override
        public void onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {

        }

        @Override
        public void onIsNull(Reference reference) {

        }

        @Override
        public void onInvoke(String method, Value target, List<Value> argumentsList) {

        }

        @Override
        public void onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType) {

        }

        @Override
        public void onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {

        }

        @Override
        public void onSearch(String searchString, List<Reference> optionalReferencesList) {

        }
    }
}
