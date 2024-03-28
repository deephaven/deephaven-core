//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.json.AnyOptions;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.BigDecimalOptions;
import io.deephaven.json.BigIntegerOptions;
import io.deephaven.json.BoolOptions;
import io.deephaven.json.ByteOptions;
import io.deephaven.json.CharOptions;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.FloatOptions;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectKvOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ShortOptions;
import io.deephaven.json.SkipOptions;
import io.deephaven.json.StringOptions;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.TypedObjectOptions;
import io.deephaven.json.ValueOptions.Visitor;
import io.deephaven.json.jackson.IterableOpt.What;

final class IterableOpt implements Visitor<What> {

    interface What {

    }

    @Override
    public What visit(ArrayOptions array) {
        return null;
    }

    @Override
    public What visit(ObjectKvOptions objectKv) {
        return null;
    }

    @Override
    public What visit(StringOptions _string) {
        return null;
    }

    @Override
    public What visit(BoolOptions _bool) {
        return null;
    }

    @Override
    public What visit(CharOptions _char) {
        return null;
    }

    @Override
    public What visit(ByteOptions _byte) {
        return null;
    }

    @Override
    public What visit(ShortOptions _short) {
        return null;
    }

    @Override
    public What visit(IntOptions _int) {
        return null;
    }

    @Override
    public What visit(LongOptions _long) {
        return null;
    }

    @Override
    public What visit(FloatOptions _float) {
        return null;
    }

    @Override
    public What visit(DoubleOptions _double) {
        return null;
    }

    @Override
    public What visit(ObjectOptions object) {
        return null;
    }

    @Override
    public What visit(InstantOptions instant) {
        return null;
    }

    @Override
    public What visit(InstantNumberOptions instantNumber) {
        return null;
    }

    @Override
    public What visit(BigIntegerOptions bigInteger) {
        return null;
    }

    @Override
    public What visit(BigDecimalOptions bigDecimal) {
        return null;
    }

    @Override
    public What visit(SkipOptions skip) {
        return null;
    }

    @Override
    public What visit(TupleOptions tuple) {
        return null;
    }

    @Override
    public What visit(TypedObjectOptions typedObject) {
        return null;
    }

    @Override
    public What visit(LocalDateOptions localDate) {
        return null;
    }

    @Override
    public What visit(AnyOptions any) {
        return null;
    }
}
