//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.filter;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AndCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Condition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.InvokeCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.NotCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.OrCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Reference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SearchCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Value;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.*;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Describes a filter which can be applied to a table. Replacing these instances may be more expensive than reusing
 * them. These instances are immutable - all operations that compose them to build bigger expressions return a new
 * instance.
 */
@JsType(namespace = "dh")
public class FilterCondition {

    @JsIgnore
    private final Condition descriptor;

    /**
     * a filter condition invoking a static function with the given parameters. Currently supported Deephaven static
     * functions:
     * <ul>
     * <li><b>inRange</b>: Given three comparable values, returns true if the first is less than the second but greater
     * than the third</li>
     * <li><b>isInf</b>:Returns true if the given number is <i>infinity</i></li>
     * <li><b>isNaN</b>:Returns true if the given number is <i>not a number</i></li>
     * <li><b>isNormal</b>:Returns true if the given number <i>is not null</i>, <i>is not infinity</i>, and <i>is not
     * "not a number"</i></li>
     * <li><b>startsWith</b>:Returns true if the first string starts with the second string</li>
     * <li><b>endsWith</b>Returns true if the first string ends with the second string</li>
     * <li><b>matches</b>:Returns true if the first string argument matches the second string used as a Java regular
     * expression</li>
     * <li><b>contains</b>:Returns true if the first string argument contains the second string as a substring</li>
     * <li><b>in</b>:Returns true if the first string argument can be found in the second array argument.
     * <p>
     * Note that the array can only be specified as a column reference at this time - typically the `FilterValue.in`
     * method should be used in other cases
     * </p>
     * </li>
     * </ul>
     *
     * @param function
     * @param args
     * @return dh.FilterCondition
     */
    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition invoke(String function, FilterValue... args) {
        InvokeCondition invoke = new InvokeCondition();
        invoke.setMethod(function);
        invoke.setArgumentsList(Arrays.stream(args).map(v -> v.descriptor).toArray(Value[]::new));

        Condition c = new Condition();
        c.setInvoke(invoke);

        return createAndValidate(c);
    }

    /**
     * a filter condition which will check if the given value can be found in any supported column on whatever table
     * this FilterCondition is passed to. This FilterCondition is somewhat unique in that it need not be given a column
     * instance, but will adapt to any table. On numeric columns, with a value passed in which can be parsed as a
     * number, the column will be filtered to numbers which equal, or can be "rounded" effectively to this number. On
     * String columns, the given value will match any column which contains this string in a case-insensitive search. An
     * optional second argument can be passed, an array of `FilterValue` from the columns to limit this search to (see
     * {@link Column#filter}).
     * 
     * @param value
     * @param columns
     * @return dh.FilterCondition
     */
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

    /**
     * the opposite of this condition
     * 
     * @return FilterCondition
     */
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

    /**
     * a condition representing the current condition logically ANDed with the other parameters
     * 
     * @param filters
     * @return FilterCondition
     */
    public FilterCondition and(FilterCondition... filters) {
        AndCondition and = new AndCondition();
        and.setFiltersList(Stream.concat(Stream.of(descriptor), Arrays.stream(filters).map(v -> v.descriptor))
                .toArray(Condition[]::new));

        Condition c = new Condition();
        c.setAnd(and);

        return createAndValidate(c);
    }


    /**
     * a condition representing the current condition logically ORed with the other parameters
     * 
     * @param filters
     * @return FilterCondition.
     */
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

    /**
     * a string suitable for debugging showing the details of this condition.
     * 
     * @return String.
     */
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

        // TODO (deephaven-core#723): implement a reasonable equality method; comparing pb serialization is expensive
        final Uint8Array mBinary = descriptor.serializeBinary();
        final Uint8Array oBinary = that.descriptor.serializeBinary();
        if (mBinary.length != oBinary.length) {
            return false;
        }

        for (int i = 0; i < mBinary.length; ++i) {
            if (!Objects.equals(mBinary.getAt(i), oBinary.getAt(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    @JsIgnore
    public int hashCode() {
        return descriptor.hashCode();
    }
}
