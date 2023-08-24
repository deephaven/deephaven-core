/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.PrimitiveFunction;
import io.deephaven.functions.ShortFunction;
import io.deephaven.functions.TypedFunction;

class DhNullableTypeTransform {

    public static <X> TypedFunction<X> of(TypedFunction<X> f) {
        return NullableTypeVisitor.of(f);
    }

    public static <X> TypedFunction<X> of(PrimitiveFunction<X> f) {
        return NullableTypeVisitor.of(f);
    }

    private enum NullableTypeVisitor implements TypedFunction.Visitor<Object, TypedFunction<Object>>,
            PrimitiveFunction.Visitor<Object, TypedFunction<Object>> {
        INSTANCE;

        public static <X> TypedFunction<X> of(TypedFunction<X> f) {
            // noinspection unchecked
            return f.walk((TypedFunction.Visitor<X, TypedFunction<X>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        public static <X> TypedFunction<X> of(PrimitiveFunction<X> f) {
            // noinspection unchecked
            return f.walk((PrimitiveFunction.Visitor<X, TypedFunction<X>>) (PrimitiveFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public TypedFunction<Object> visit(PrimitiveFunction<Object> f) {
            return of(f);
        }

        @Override
        public TypedFunction<Object> visit(ObjectFunction<Object, ?> f) {
            return f;
        }

        @Override
        public ObjectFunction<Object, Boolean> visit(BooleanFunction<Object> f) {
            // BooleanFunction is the only function / primitive type that doesn't natively have a "null" type.
            return BoxTransform.of(f);
        }

        @Override
        public TypedFunction<Object> visit(CharFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(ByteFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(ShortFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(IntFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(LongFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(FloatFunction<Object> f) {
            return f;
        }

        @Override
        public TypedFunction<Object> visit(DoubleFunction<Object> f) {
            return f;
        }
    }
}
