//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectProcessorStrictTest {

    @Test
    public void testNoDoubleWrapping() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        ObjectProcessor<Object> strict2 = ObjectProcessor.strict(strict);
        assertThat(strict2).isSameAs(strict);
    }

    @Test
    public void testCorrectOutputTypes() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        assertThat(strict.outputTypes()).containsExactly(Type.intType());
    }

    @Test
    public void testNpeOnNullIn() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        try (WritableIntChunk<Any> c1 = WritableIntChunk.makeWritableChunk(1)) {
            c1.setSize(0);
            try {
                strict.processAll(null, List.of(c1));
                failBecauseExceptionWasNotThrown(NullPointerException.class);
            } catch (NullPointerException e) {
                // expected
            }
        }
    }

    @Test
    public void testNpeOnNullOut() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        try (WritableObjectChunk<Object, Any> in = WritableObjectChunk.makeWritableChunk(1)) {
            try {
                strict.processAll(in, null);
                failBecauseExceptionWasNotThrown(NullPointerException.class);
            } catch (NullPointerException e) {
                // expected
            }
        }
    }

    @Test
    public void testIncorrectNumOutChunks() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        try (
                WritableObjectChunk<Object, Any> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<Any> c1 = WritableIntChunk.makeWritableChunk(1);
                WritableDoubleChunk<Any> c2 = WritableDoubleChunk.makeWritableChunk(1)) {
            c1.setSize(0);
            c2.setSize(0);
            try {
                strict.processAll(in, List.of(c1, c2));
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("Improper number of out chunks");
            }
        }
    }

    @Test
    public void testIncorrectChunkType() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        try (
                WritableObjectChunk<Object, Any> in = WritableObjectChunk.makeWritableChunk(1);
                WritableDoubleChunk<Any> c1 = WritableDoubleChunk.makeWritableChunk(1)) {
            c1.setSize(0);
            try {
                strict.processAll(in, List.of(c1));
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("Improper ChunkType");
            }
        }
    }

    @Test
    public void testNotEnoughOutputOutputSize() {
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType()), false);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        try (
                WritableObjectChunk<Object, Any> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<Any> c1 = WritableIntChunk.makeWritableChunk(1)) {
            try {
                c1.setSize(c1.capacity());
                strict.processAll(in, List.of(c1));
                failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
            } catch (IllegalArgumentException e) {
                assertThat(e).hasMessageContaining("out chunk does not have enough remaining capacity");
            }
        }
    }

    @Test
    public void testDifferentOutChunkSizes() {
        // Note: the ObjectProcesser does *not* guarantee that the output chunk sizes will all be the same.
        // In the future if we add additional guarantees about the ObjectProcessor in these regards, we'll update this
        // test and the strict impl as necessary.
        ObjectProcessor<Object> delegate = ObjectProcessor.noop(List.of(Type.intType(), Type.intType()), true);
        ObjectProcessor<Object> strict = ObjectProcessor.strict(delegate);
        try (
                WritableObjectChunk<Object, Any> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<Any> c1 = WritableIntChunk.makeWritableChunk(2);
                WritableIntChunk<Any> c2 = WritableIntChunk.makeWritableChunk(2)) {

            c1.setSize(0);
            c1.set(0, 0);

            c2.setSize(1);
            c2.set(1, 0);

            strict.processAll(in, List.of(c1, c2));

            assertThat(c1.size()).isEqualTo(1);
            assertThat(c1.get(0)).isEqualTo(QueryConstants.NULL_INT);

            assertThat(c2.size()).isEqualTo(2);
            assertThat(c2.get(1)).isEqualTo(QueryConstants.NULL_INT);
        }
    }

    @Test
    public void testNPEOnNullDelegateOutputTypes() {
        try {
            ObjectProcessor.strict(new ObjectProcessor<>() {
                @Override
                public List<Type<?>> outputTypes() {
                    return null;
                }

                @Override
                public void processAll(ObjectChunk<?, ?> in, List<WritableChunk<?>> out) {

                }
            });
            failBecauseExceptionWasNotThrown(NullPointerException.class);
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testBadDelegateOutputTypes() {
        ObjectProcessor<Object> strict = ObjectProcessor.strict(new ObjectProcessor<>() {
            private final List<Type<?>> outputTypes = new ArrayList<>(List.of(Type.intType()));

            @Override
            public int outputSize() {
                return 1;
            }

            @Override
            public List<Type<?>> outputTypes() {
                try {
                    return List.copyOf(outputTypes);
                } finally {
                    outputTypes.clear();
                }
            }

            @Override
            public void processAll(ObjectChunk<?, ?> in, List<WritableChunk<?>> out) {
                // don't care about impl.
            }
        });
        try {
            strict.outputTypes();
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (UncheckedDeephavenException e) {
            assertThat(e).hasMessageContaining("Implementation is returning a different list of outputTypes");
        }
    }

    @Test
    public void testBadDelegateProcessAll() {
        ObjectProcessor<Object> strict = ObjectProcessor.strict(new ObjectProcessor<>() {
            @Override
            public List<Type<?>> outputTypes() {
                return List.of(Type.intType());
            }

            @Override
            public void processAll(ObjectChunk<?, ?> in, List<WritableChunk<?>> out) {
                // Bad impl
                // don't increment out sizes as appropriate
            }
        });
        try (
                WritableObjectChunk<Object, Any> in = WritableObjectChunk.makeWritableChunk(1);
                WritableIntChunk<Any> c1 = WritableIntChunk.makeWritableChunk(1)) {
            try {
                c1.setSize(0);
                strict.processAll(in, List.of(c1));
                failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
            } catch (UncheckedDeephavenException e) {
                assertThat(e).hasMessageContaining("Implementation did not increment chunk size correctly");
            }
        }
    }

    @Test
    public void testBadDelegateOutputSize() {
        try {
            ObjectProcessor.strict(new ObjectProcessor<>() {
                @Override
                public int outputSize() {
                    return 2;
                }

                @Override
                public List<Type<?>> outputTypes() {
                    return List.of(Type.intType());
                }

                @Override
                public void processAll(ObjectChunk<?, ?> in, List<WritableChunk<?>> out) {
                    // ignore
                }
            });
            failBecauseExceptionWasNotThrown(IllegalAccessException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining(
                    "Inconsistent size. delegate.outputSize()=2, delegate.outputTypes().size()=1");
        }
    }
}
