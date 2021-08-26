package io.deephaven.web.client.api.filter;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.*;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.*;

import java.util.Arrays;
import java.util.stream.Stream;

@JsType(namespace = "dh")
public class FilterCondition {

    @JsIgnore
    private final Condition descriptor;

    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition invoke(String function, FilterValue... args) {
        InvokeCondition invoke = new InvokeCondition();
        invoke.setMethod(function);
        invoke.setArgumentsList(Arrays.stream(args).map(v -> v.descriptor).toArray(Value[]::new));

        Condition c = new Condition();
        c.setInvoke(invoke);

        return createAndValidate(c);
    }

    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition search(FilterValue value, @JsOptional FilterValue[] columns) {
        SearchCondition search = new SearchCondition();
        search.setSearchString(value.descriptor.getLiteral().getStringValue());
        if (columns != null) {
            search.setOptionalReferencesList(
                    Arrays.stream(columns).map(v -> v.descriptor.getReference()).toArray(Reference[]::new));
        }

        Condition c = new Condition();
        c.setSearch(search);

        return createAndValidate(c);
    }

    @JsIgnore
    public FilterCondition(Condition descriptor) {
        this.descriptor = descriptor;
    }

    public FilterCondition not() {
        NotCondition not = new NotCondition();
        not.setFilter(descriptor);

        Condition c = new Condition();
        c.setNot(not);

        return createAndValidate(c);
    }

    @JsIgnore
    protected static FilterCondition createAndValidate(Condition descriptor) {
        // TODO (deephaven-core#723) re-introduce client-side validation so that a client knows right away when
        // they build something invalid
        return new FilterCondition(descriptor);
    }

    public FilterCondition and(FilterCondition... filters) {
        AndCondition and = new AndCondition();
        and.setFiltersList(Stream.concat(Stream.of(descriptor), Arrays.stream(filters).map(v -> v.descriptor))
                .toArray(Condition[]::new));

        Condition c = new Condition();
        c.setAnd(and);

        return createAndValidate(c);
    }


    public FilterCondition or(FilterCondition... filters) {
        OrCondition or = new OrCondition();
        or.setFiltersList(Stream.concat(Stream.of(descriptor), Arrays.stream(filters).map(v -> v.descriptor))
                .toArray(Condition[]::new));

        Condition c = new Condition();
        c.setOr(or);

        return createAndValidate(c);
    }

    @JsIgnore
    public Condition makeDescriptor() {
        // these are immutable, so we can just return the instance
        return descriptor;
    }

    @JsMethod
    public String toString() {
        // TODO (deephaven-core#723) implement a readable tostring rather than turning the pb object into a string
        return descriptor.toString();
    }

    @JsProperty
    public JsArray<Column> getColumns() {
        return new JsArray<>();
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final FilterCondition that = (FilterCondition) o;

        return descriptor.equals(that.descriptor);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        return descriptor.hashCode();
    }
}
