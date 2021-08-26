package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FilterTestUtils {
    public static Condition and(Condition... filterDescriptors) {
        return NormalizeFilterUtil.doAnd(Arrays.asList(filterDescriptors));
    }

    public static Condition or(Condition... filterDescriptors) {
        return NormalizeFilterUtil.doOr(Arrays.asList(filterDescriptors));
    }

    public static Condition not(Condition filterDescriptor) {
        return NormalizeFilterUtil.doInvert(filterDescriptor);
    }


    public static Value reference(String columnName) {
        return Value.newBuilder().setReference(Reference.newBuilder().setColumnName(columnName).build()).build();
    }

    public static Value literal(String stringValue) {
        return Value.newBuilder().setLiteral(Literal.newBuilder().setStringValue(stringValue).build()).build();
    }

    public static List<Value> literals(String... stringValues) {
        return Stream.of(stringValues).map(FilterTestUtils::literal).collect(Collectors.toList());
    }

    public static Value literal(long longValue) {
        return Value.newBuilder().setLiteral(Literal.newBuilder().setLongValue(longValue).build()).build();
    }

    public static Value literal(int intValue) {
        return literal((double) intValue);
    }

    public static Value literal(double doubleValue) {
        return Value.newBuilder().setLiteral(Literal.newBuilder().setDoubleValue(doubleValue).build()).build();
    }

    public static List<Value> literals(int... intValues) {
        return IntStream.of(intValues).mapToObj(i -> literal((double) i)).collect(Collectors.toList());
    }

    public static List<Value> literals(double... doubleValues) {
        return DoubleStream.of(doubleValues).mapToObj(FilterTestUtils::literal).collect(Collectors.toList());
    }

    public static Condition eq(String columnName, int value) {
        return NormalizeFilterUtil.doComparison(CompareCondition.CompareOperation.EQUALS, CaseSensitivity.MATCH_CASE,
                reference(columnName), literal(value));
    }

    public static Condition notEq(String columnName, int value) {
        return NormalizeFilterUtil.doComparison(CompareCondition.CompareOperation.NOT_EQUALS,
                CaseSensitivity.MATCH_CASE, reference(columnName), literal(value));
    }

    public static Condition in(String columnName, int... values) {
        return NormalizeFilterUtil.doIn(reference(columnName), literals(values), CaseSensitivity.MATCH_CASE,
                MatchType.REGULAR);
    }

    public static Condition in(Value lhs, Value... rhs) {
        return NormalizeFilterUtil.doIn(lhs, Arrays.asList(rhs), CaseSensitivity.MATCH_CASE, MatchType.REGULAR);
    }

    public static Condition inICase(Value lhs, Value... rhs) {
        return NormalizeFilterUtil.doIn(lhs, Arrays.asList(rhs), CaseSensitivity.IGNORE_CASE, MatchType.REGULAR);
    }

    public static Condition notIn(String columnName, int... values) {
        return NormalizeFilterUtil.doIn(reference(columnName), literals(values), CaseSensitivity.MATCH_CASE,
                MatchType.INVERTED);
    }

    public static Condition notIn(Value lhs, Value... rhs) {
        return NormalizeFilterUtil.doIn(lhs, Arrays.asList(rhs), CaseSensitivity.MATCH_CASE, MatchType.INVERTED);
    }

    public static Condition notInICase(Value lhs, Value... rhs) {
        return NormalizeFilterUtil.doIn(lhs, Arrays.asList(rhs), CaseSensitivity.IGNORE_CASE, MatchType.INVERTED);
    }

    public static Condition compare(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
        return NormalizeFilterUtil.doComparison(operation, CaseSensitivity.MATCH_CASE, lhs, rhs);
    }

    public static Condition compare(CompareCondition.CompareOperation operation, CaseSensitivity caseSensitivity,
            Value lhs, Value rhs) {
        return NormalizeFilterUtil.doComparison(operation, caseSensitivity, lhs, rhs);
    }

    public static Condition invoke(String method, Value target, Value... filterDescriptors) {
        return NormalizeFilterUtil.doInvoke(method, target, Arrays.asList(filterDescriptors));
    }
}
