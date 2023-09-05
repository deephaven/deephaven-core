/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToPrimitiveFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.functions.TypedFunction;

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
