//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.json.jackson.LongValueProcessor.ToLong;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_LONG_ARRAY;

final class LongRepeaterImpl extends RepeaterProcessorBase<long[]> {

    private final ToLong toLong;

    public LongRepeaterImpl(ToLong toLong, boolean allowMissing, boolean allowNull,
            Consumer<? super long[]> consumer) {
        super(consumer, allowMissing, allowNull, null, null);
        this.toLong = Objects.requireNonNull(toLong);
    }

    @Override
    public LongArrayContext newContext() {
        return new LongArrayContext();
    }

    final class LongArrayContext extends RepeaterContextBase {
        private long[] arr = EMPTY_LONG_ARRAY;
        private int len = 0;

        @Override
        public void processElement(JsonParser parser, int index) throws IOException {
            if (index != len) {
                throw new IllegalStateException();
            }
            arr = ArrayUtil.put(arr, len, toLong.parseValue(parser));
            ++len;
        }

        @Override
        public void processElementMissing(JsonParser parser, int index) throws IOException {
            if (index != len) {
                throw new IllegalStateException();
            }
            arr = ArrayUtil.put(arr, len, toLong.parseMissing(parser));
            ++len;
        }

        @Override
        public long[] onDone(int length) {
            if (length != len) {
                throw new IllegalStateException();
            }
            return arr.length == len ? arr : Arrays.copyOf(arr, len);
        }
    }
}
