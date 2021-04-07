package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.ArrayList;
import java.util.List;

public interface ReplacingFilterVisitor {
    Condition onAnd(List<Condition> filtersList);
    Condition onOr(List<Condition> filtersList);
    Condition onNot(Condition filter);

    Condition onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs);

    Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType);

    Condition onIsNull(Reference reference);

    Condition onInvoke(String method, Value target, List<Value> argumentsList);

    Condition onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType);
    Condition onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType);

    Condition onSearch(String searchString, List<Reference> optionalReferencesList);

    static Condition accept(Condition condition, ReplacingFilterVisitor visitor) {
        switch (condition.getDataCase()) {
            case AND:
                return visitor.onAnd(condition.getAnd().getFiltersList());
            case OR:
                return visitor.onOr(condition.getAnd().getFiltersList());
            case NOT:
                return visitor.onNot(condition.getNot().getFilter());
            case COMPARE:
                return visitor.onComparison(condition.getCompare().getOperation(), condition.getCompare().getLhs(), condition.getCompare().getRhs());
            case IN:
                return visitor.onIn(condition.getIn().getTarget(), condition.getIn().getCandidatesList(), condition.getIn().getCaseSensitivity(), condition.getIn().getMatchType());
            case INVOKE:
                return visitor.onInvoke(condition.getInvoke().getMethod(), condition.getInvoke().getTarget(), condition.getInvoke().getArgumentsList());
            case ISNULL:
                return visitor.onIsNull(condition.getIsNull().getReference());
            case MATCHES:
                return visitor.onMatches(condition.getMatches().getReference(), condition.getMatches().getRegex(), condition.getMatches().getCaseSensitivity(), condition.getMatches().getMatchType());
            case CONTAINS:
                return visitor.onContains(condition.getContains().getReference(), condition.getContains().getSearchString(), condition.getContains().getCaseSensitivity(), condition.getContains().getMatchType());
            case SEARCH:
                return visitor.onSearch(condition.getSearch().getSearchString(), condition.getSearch().getOptionalReferencesList());
            case DATA_NOT_SET:
            default:
                throw new IllegalStateException("Unsupported condition " + condition);
        }
    }
    public abstract class AbstractReplacingFilterVisitor implements ReplacingFilterVisitor {

        @Override
        public Condition onAnd(List<Condition> filtersList) {
            return Condition.newBuilder().setAnd(AndCondition.newBuilder()
                    .addAllFilters(visitChildren(filtersList))
                    .build()).build();
        }

        protected List<Condition> visitChildren(List<Condition> filtersList) {
            List<Condition> replacements = new ArrayList<>();
            for (Condition condition : filtersList) {
                Condition replacement = accept(condition, this);
                if (replacement != null) {
                    replacements.add(replacement);
                }
            }
            return replacements;
        }

        @Override
        public Condition onOr(List<Condition> filtersList) {
            return Condition.newBuilder().setOr(OrCondition.newBuilder()
                    .addAllFilters(visitChildren(filtersList))
                    .build()).build();
        }

        @Override
        public Condition onNot(Condition filter) {
            Condition replacement = accept(filter, this);
            if (replacement == null) {
                return null;
            }
            return Condition.newBuilder().setNot(NotCondition.newBuilder()
                    .setFilter(replacement)
                    .build()).build();
        }

        @Override
        public Condition onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
            return Condition.newBuilder().setCompare(CompareCondition.newBuilder()
                    .setOperation(operation)
                    .setLhs(lhs)
                    .setRhs(rhs)
                    .build()).build();
        }

        @Override
        public Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
            return Condition.newBuilder().setIn(InCondition.newBuilder()
                    .setTarget(target)
                    .addAllCandidates(candidatesList)
                    .setCaseSensitivity(caseSensitivity)
                    .setMatchType(matchType)
                    .build()).build();
        }

        @Override
        public Condition onIsNull(Reference reference) {
            return Condition.newBuilder().setIsNull(IsNullCondition.newBuilder()
                    .setReference(reference)
                    .build()).build();
        }

        @Override
        public Condition onInvoke(String method, Value target, List<Value> argumentsList) {
            return Condition.newBuilder().setInvoke(InvokeCondition.newBuilder()
                    .setMethod(method)
                    .setTarget(target)
                    .addAllArguments(argumentsList)
                    .build()).build();
        }

        @Override
        public Condition onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType) {
            return Condition.newBuilder().setContains(ContainsCondition.newBuilder()
                    .setReference(reference)
                    .setSearchString(searchString)
                    .setCaseSensitivity(caseSensitivity)
                    .setMatchType(matchType)
                    .build()).build();
        }

        @Override
        public Condition onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {
            return Condition.newBuilder().setMatches(MatchesCondition.newBuilder()
                    .setReference(reference)
                    .setRegex(regex)
                    .setCaseSensitivity(caseSensitivity)
                    .setMatchType(matchType)
                    .build()).build();
        }

        @Override
        public Condition onSearch(String searchString, List<Reference> optionalReferencesList) {
            return Condition.newBuilder().setSearch(SearchCondition.newBuilder()
                    .setSearchString(searchString)
                    .addAllOptionalReferences(optionalReferencesList)
                    .build()).build();
        }
    }
}
