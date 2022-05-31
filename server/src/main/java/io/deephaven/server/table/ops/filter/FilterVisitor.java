package io.deephaven.server.table.ops.filter;

import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.Value;

import java.util.List;

public interface FilterVisitor<R> {
    R onAnd(List<Condition> filtersList);

    R onOr(List<Condition> filtersList);

    R onNot(Condition filter);

    R onComparison(CompareCondition.CompareOperation operation, CaseSensitivity caseSensitivity, Value lhs, Value rhs);

    R onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType);

    R onIsNull(Reference reference);

    R onInvoke(String method, Value target, List<Value> argumentsList);

    R onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType);

    R onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType);

    R onSearch(String searchString, List<Reference> optionalReferencesList);

    static <R> R accept(Condition condition, FilterVisitor<R> visitor) {
        switch (condition.getDataCase()) {
            case AND:
                return visitor.onAnd(condition.getAnd().getFiltersList());
            case OR:
                return visitor.onOr(condition.getOr().getFiltersList());
            case NOT:
                return visitor.onNot(condition.getNot().getFilter());
            case COMPARE:
                return visitor.onComparison(condition.getCompare().getOperation(),
                        condition.getCompare().getCaseSensitivity(), condition.getCompare().getLhs(),
                        condition.getCompare().getRhs());
            case IN:
                return visitor.onIn(condition.getIn().getTarget(), condition.getIn().getCandidatesList(),
                        condition.getIn().getCaseSensitivity(), condition.getIn().getMatchType());
            case INVOKE:
                return visitor.onInvoke(condition.getInvoke().getMethod(), condition.getInvoke().getTarget(),
                        condition.getInvoke().getArgumentsList());
            case IS_NULL:
                return visitor.onIsNull(condition.getIsNull().getReference());
            case MATCHES:
                return visitor.onMatches(condition.getMatches().getReference(), condition.getMatches().getRegex(),
                        condition.getMatches().getCaseSensitivity(), condition.getMatches().getMatchType());
            case CONTAINS:
                return visitor.onContains(condition.getContains().getReference(),
                        condition.getContains().getSearchString(), condition.getContains().getCaseSensitivity(),
                        condition.getContains().getMatchType());
            case SEARCH:
                return visitor.onSearch(condition.getSearch().getSearchString(),
                        condition.getSearch().getOptionalReferencesList());
            case DATA_NOT_SET:
            default:
                throw new IllegalStateException("Unsupported condition " + condition);
        }
    }
}
