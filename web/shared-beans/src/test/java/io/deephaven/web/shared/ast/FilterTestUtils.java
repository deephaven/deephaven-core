package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FilterTestUtils {
    public static FilterDescriptor and(FilterDescriptor... filterDescriptors) {
        return node(FilterDescriptor.FilterOperation.AND, filterDescriptors);
    }

    public static FilterDescriptor or(FilterDescriptor... filterDescriptors) {
        return node(FilterDescriptor.FilterOperation.OR, filterDescriptors);
    }

    public static FilterDescriptor not(FilterDescriptor filterDescriptor) {
        return node(FilterDescriptor.FilterOperation.NOT, filterDescriptor);
    }

    // public static FilterDescriptor eq(String columnName, String value) {
    // return node(FilterDescriptor.FilterOperation.EQ, reference(columnName), literal(value));
    // }
    // public static FilterDescriptor notEq(String columnName, String value) {
    // return node(FilterDescriptor.FilterOperation.NEQ, reference(columnName), literal(value));
    // }
    // public static FilterDescriptor in(String columnName, String... values) {
    // return node(FilterDescriptor.FilterOperation.NOT_IN, concat(reference(columnName), literals(values)));
    // }
    // public static FilterDescriptor notIn(String columnName, String... values) {
    // return node(FilterDescriptor.FilterOperation.IN, concat(reference(columnName), literals(values)));
    // }

    public static FilterDescriptor reference(String columnName) {
        FilterDescriptor reference = new FilterDescriptor();
        reference.setValue(columnName);
        reference.setOperation(FilterDescriptor.FilterOperation.REFERENCE);

        return reference;
    }

    public static FilterDescriptor literal(String stringValue) {
        FilterDescriptor literal = new FilterDescriptor();
        literal.setValue(stringValue);
        literal.setType(FilterDescriptor.ValueType.String);
        literal.setOperation(FilterDescriptor.FilterOperation.LITERAL);
        return literal;
    }

    public static FilterDescriptor[] literals(String... stringValues) {
        return Stream.of(stringValues).map(FilterTestUtils::literal).toArray(FilterDescriptor[]::new);
    }


    public static FilterDescriptor eq(String columnName, int value) {
        return node(FilterDescriptor.FilterOperation.EQ, reference(columnName), literal((double) value));
    }

    public static FilterDescriptor notEq(String columnName, int value) {
        return node(FilterDescriptor.FilterOperation.NEQ, reference(columnName), literal((double) value));
    }

    public static FilterDescriptor in(String columnName, int... values) {
        return node(FilterDescriptor.FilterOperation.IN, concat(reference(columnName), literals(values)));
    }

    public static FilterDescriptor notIn(String columnName, int... values) {
        return node(FilterDescriptor.FilterOperation.NOT_IN, concat(reference(columnName), literals(values)));
    }

    public static FilterDescriptor literal(long longValue) {
        FilterDescriptor literal = new FilterDescriptor();
        literal.setValue(String.valueOf(longValue));
        literal.setType(FilterDescriptor.ValueType.Long);
        literal.setOperation(FilterDescriptor.FilterOperation.LITERAL);
        return literal;
    }

    public static FilterDescriptor literal(int intValue) {
        return literal((double) intValue);
    }

    public static FilterDescriptor literal(double doubleValue) {
        return numericLiteral(String.valueOf(doubleValue));
    }

    private static FilterDescriptor numericLiteral(String numericAsString) {
        FilterDescriptor literal = new FilterDescriptor();
        literal.setValue(numericAsString);
        literal.setType(FilterDescriptor.ValueType.Number);
        literal.setOperation(FilterDescriptor.FilterOperation.LITERAL);
        return literal;
    }

    public static FilterDescriptor[] literals(int... intValues) {
        return IntStream.of(intValues).mapToObj(i -> literal((double) i)).toArray(FilterDescriptor[]::new);
    }

    public static FilterDescriptor[] literals(double... doubleValues) {
        return DoubleStream.of(doubleValues).mapToObj(FilterTestUtils::literal).toArray(FilterDescriptor[]::new);
    }

    private static FilterDescriptor[] concat(FilterDescriptor first, FilterDescriptor... arr) {
        return Stream.concat(
                Stream.of(first),
                Arrays.stream(arr)).toArray(FilterDescriptor[]::new);
    }


    public static FilterDescriptor invoke(String method, FilterDescriptor... filterDescriptors) {
        FilterDescriptor descriptor = node(FilterDescriptor.FilterOperation.INVOKE, filterDescriptors);
        descriptor.setValue(method);

        return descriptor;
    }

    public static FilterDescriptor node(FilterDescriptor.FilterOperation op, FilterDescriptor... filterDescriptors) {
        FilterDescriptor filterDescriptor = new FilterDescriptor();
        filterDescriptor.setOperation(op);
        filterDescriptor.setChildren(filterDescriptors);
        return filterDescriptor;
    }


}
