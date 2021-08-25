package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.List;

public abstract class AbstractNormalizeFilters implements FilterVisitor<Condition> {
    @Override
    public Condition onAnd(List<Condition> filtersList) {
        return NormalizeFilterUtil.doAnd(filtersList, this);
    }

    @Override
    public Condition onOr(List<Condition> filtersList) {
        return NormalizeFilterUtil.doOr(filtersList, this);
    }

    @Override
    public Condition onNot(Condition filter) {
        return NormalizeFilterUtil.doInvert(filter, this);
    }

    @Override
    public Condition onComparison(CompareCondition.CompareOperation operation, CaseSensitivity caseSensitivity,
            Value lhs, Value rhs) {
        return NormalizeFilterUtil.doComparison(operation, caseSensitivity, lhs, rhs);
    }

    @Override
    public Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity,
            MatchType matchType) {
        return NormalizeFilterUtil.doIn(target, candidatesList, caseSensitivity, matchType);
    }

    @Override
    public Condition onIsNull(Reference reference) {
        return NormalizeFilterUtil.doIsNull(reference);
    }

    @Override
    public Condition onInvoke(String method, Value target, List<Value> argumentsList) {
        return NormalizeFilterUtil.doInvoke(method, target, argumentsList);
    }

    @Override
    public Condition onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity,
            MatchType matchType) {
        return NormalizeFilterUtil.doContains(reference, searchString, caseSensitivity, matchType);
    }

    @Override
    public Condition onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity,
            MatchType matchType) {
        return NormalizeFilterUtil.doMatches(reference, regex, caseSensitivity, matchType);
    }

    @Override
    public Condition onSearch(String searchString, List<Reference> optionalReferencesList) {
        return NormalizeFilterUtil.doSearch(searchString, optionalReferencesList);
    }
}
