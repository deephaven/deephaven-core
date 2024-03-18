//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.function.ToBooleanFunction;
import io.deephaven.function.ToByteFunction;
import io.deephaven.function.ToCharFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToPrimitiveFunction;
import io.deephaven.function.ToShortFunction;
import io.deephaven.function.TypedFunction;

class DhNullableTypeTransform {

    public static <X> TypedFunction<X> of(TypedFunction<X> f) {
        return NullableTypeVisitor.of(f);
    }

    public static <X> TypedFunction<X> of(ToPrimitiveFunction<X> f) {
        return NullableTypeVisitor.of(f);
    }

    private enum NullableTypeVisitor implements TypedFunction.Visitor<Object, TypedFunction<Object>>,
            ToPrimitiveFunction.Visitor<Object, TypedFunction<Object>> {
        INSTANCE;

        public static <X> TypedFunction<X> of(TypedFunction<X> f) {
            // noinspection unchecked
            return f.walk((TypedFunction.Visitor<X, TypedFunction<X>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        public static <X> TypedFunction<X> of(ToPrimitiveFunction<X> f) {
            // noinspection unchecked
            return f.walk(
                    (ToPrimitiveFunction.Visitor<X, TypedFunction<X>>) (ToPrimitiveFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public TypedFunction<Object> visit(ToPrimitiveFunction<Object> f) {
            return of(f);
        }

        @Override
        public TypedFunction<Object> visit(ToObjectFunction<Object, ?> f) {
            return f;
        }

        @Override
        public ToObjectFunction<Object, Boolean> visit(ToBooleanFunction<Object> f) {
            // BooleanFunction is the only function / primitive type that doesn't natively have a "null" type.
            return BoxTransform.of(f);
        }

        @Override
        public ToCharFunction<Object> visit(ToCharFunction<Object> f) {
            return f;
        }

        @Override
        public ToByteFunction<Object> visit(ToByteFunction<Object> f) {
            return f;
        }

        @Override
        public ToShortFunction<Object> visit(ToShortFunction<Object> f) {
            return f;
        }

        @Override
        public ToIntFunction<Object> visit(ToIntFunction<Object> f) {
            return f;
        }

        @Override
        public ToLongFunction<Object> visit(ToLongFunction<Object> f) {
            return f;
        }

        @Override
        public ToFloatFunction<Object> visit(ToFloatFunction<Object> f) {
            return f;
        }

        @Override
        public ToDoubleFunction<Object> visit(ToDoubleFunction<Object> f) {
            return f;
        }
    }
}
