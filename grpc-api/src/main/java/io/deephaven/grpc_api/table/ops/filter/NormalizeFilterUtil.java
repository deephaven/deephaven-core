package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.List;
import java.util.stream.Collectors;

public class NormalizeFilterUtil {
    private static List<Condition> visitChildren(List<Condition> children, FilterVisitor<Condition> visitor) {
        // note: don't bother with the possibility of null returns anymore because it doesn't really make sense IMO
        return children.stream().map(c -> FilterVisitor.accept(c, visitor)).collect(Collectors.toList());
    }

    public static Condition doAnd(List<Condition> filtersList, FilterVisitor<Condition> visitor) {
        return Condition.newBuilder().setAnd(AndCondition.newBuilder()
                .addAllFilters(visitChildren(filtersList, visitor))
                .build()).build();
    }

    public static Condition doOr(List<Condition> filtersList, FilterVisitor<Condition> visitor) {
        return Condition.newBuilder().setOr(OrCondition.newBuilder()
                .addAllFilters(visitChildren(filtersList, visitor))
                .build()).build();
    }

    public static Condition doNot(Condition filter, FilterVisitor<Condition> visitor) {
        Condition replacement = FilterVisitor.accept(filter, visitor);
        return Condition.newBuilder().setNot(NotCondition.newBuilder()
                .setFilter(replacement)
                .build()).build();
    }

    public static Condition doComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
        return Condition.newBuilder().setCompare(CompareCondition.newBuilder()
                .setOperation(operation)
                .setLhs(lhs)
                .setRhs(rhs)
                .build()).build();
    }

    public static Condition doIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
        return Condition.newBuilder().setIn(InCondition.newBuilder()
                .setTarget(target)
                .addAllCandidates(candidatesList)
                .setCaseSensitivity(caseSensitivity)
                .setMatchType(matchType)
                .build()).build();
    }

    public static Condition doIsNull(Reference reference) {
        return Condition.newBuilder().setIsNull(IsNullCondition.newBuilder()
                .setReference(reference)
                .build()).build();
    }

    public static Condition doInvoke(String method, Value target, List<Value> argumentsList) {
        return Condition.newBuilder().setInvoke(InvokeCondition.newBuilder()
                .setMethod(method)
                .setTarget(target)
                .addAllArguments(argumentsList)
                .build()).build();
    }

    public static Condition doContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType) {
        return Condition.newBuilder().setContains(ContainsCondition.newBuilder()
                .setReference(reference)
                .setSearchString(searchString)
                .setCaseSensitivity(caseSensitivity)
                .setMatchType(matchType)
                .build()).build();
    }

    public static Condition doMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {
        return Condition.newBuilder().setMatches(MatchesCondition.newBuilder()
                .setReference(reference)
                .setRegex(regex)
                .setCaseSensitivity(caseSensitivity)
                .setMatchType(matchType)
                .build()).build();
    }

    public static Condition doSearch(String searchString, List<Reference> optionalReferencesList) {
        return Condition.newBuilder().setSearch(SearchCondition.newBuilder()
                .setSearchString(searchString)
                .addAllOptionalReferences(optionalReferencesList)
                .build()).build();
    }

    public static Condition doInvert(Condition condition) {
        return Condition.newBuilder().setNot(NotCondition.newBuilder().setFilter(condition)).build();
    }
}
