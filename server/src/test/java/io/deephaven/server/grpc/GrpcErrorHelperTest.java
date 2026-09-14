//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.protobuf.test.Message1v1;
import io.deephaven.protobuf.test.Message1v2;
import io.deephaven.protobuf.test.Message2v1;
import io.deephaven.protobuf.test.Message2v2;
import io.deephaven.protobuf.test.OuterV1;
import io.deephaven.protobuf.test.OuterV2;
import io.deephaven.protobuf.test.RepeatedFieldv1;
import io.deephaven.protobuf.test.RepeatedFieldv2;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class GrpcErrorHelperTest {

    @Test
    public void unknownFieldsFlatMessage() throws InvalidProtocolBufferException {
        {
            // establishing a precondition that we can decode just an id
            final Message1v2 v2OnlyId = Message1v2.newBuilder().setId(42).build();
            final Message1v1 expected = Message1v1.newBuilder().setId(42).build();
            assertThat(Message1v1.parseFrom(v2OnlyId.toByteString())).isEqualTo(expected);
        }

        {
            final Message1v2 v2 = Message1v2.newBuilder().setId(42).setName("Foo Bar").build();
            final Message1v1 v1 = Message1v1.parseFrom(v2.toByteString());
            assertThat(v1.getUnknownFields().isEmpty()).isFalse();
            assertThat(v1.getUnknownFields().hasField(Message1v2.NAME_FIELD_NUMBER));

            // v2 is fine
            GrpcErrorHelper.checkHasNoUnknownFields(v2);
            GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v2);

            // v1 has unknown fields
            try {
                GrpcErrorHelper.checkHasNoUnknownFields(v1);
                failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
            } catch (final StatusRuntimeException e) {
                assertThat(e)
                        .hasMessageContaining("io.deephaven.protobuf.test.Message1v1 has unknown field(s) with id [2]");
            }
            try {
                GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v1);
                failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
            } catch (final StatusRuntimeException e) {
                assertThat(e)
                        .hasMessageContaining("io.deephaven.protobuf.test.Message1v1 has unknown field(s) with id [2]");
            }
        }
    }

    @Test
    public void unknownFieldsNestedMessage() throws InvalidProtocolBufferException {
        {

            // establishing a precondition that we can decode just an id
            final Message2v2 v2OnlyId = Message2v2.newBuilder()
                    .setId(42)
                    .setMessage(Message1v2.newBuilder().setId(42).build())
                    .build();
            final Message2v1 expected = Message2v1.newBuilder()
                    .setId(42)
                    .setMessage(Message1v1.newBuilder().setId(42).build())
                    .build();
            assertThat(Message2v1.parseFrom(v2OnlyId.toByteString())).isEqualTo(expected);
        }

        {
            final Message2v2 v2 = Message2v2.newBuilder()
                    .setId(42)
                    .setMessage(Message1v2.newBuilder()
                            .setId(42)
                            .setName("Foo Bar")
                            .build())
                    .build();

            final Message2v1 v1 = Message2v1.parseFrom(v2.toByteString());
            assertThat(v1.getUnknownFields().isEmpty()).isTrue();
            assertThat(v1.getMessage().getUnknownFields().isEmpty()).isFalse();
            assertThat(v1.getMessage().getUnknownFields().hasField(Message1v2.NAME_FIELD_NUMBER)).isTrue();


            // v2 is fine
            GrpcErrorHelper.checkHasNoUnknownFields(v2);
            GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v2);

            // v1 no has unknown fields _itself_
            GrpcErrorHelper.checkHasNoUnknownFields(v1);
            // but, recursively, it does have known fields
            try {
                GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v1);
                failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
            } catch (final StatusRuntimeException e) {
                assertThat(e).hasMessageContaining(
                        "io.deephaven.protobuf.test.Message1v1 has unknown field(s) with id [2], topLevel=io.deephaven.protobuf.test.Message2v1, path=[io.deephaven.protobuf.test.Message2v1.message]");
            }
        }
    }

    @Test
    public void unknownFieldsDoubleNestedMessage() throws InvalidProtocolBufferException {
        {

            // establishing a precondition that we can decode just an id
            final OuterV2 v2OnlyId = OuterV2.newBuilder().setInner(Message2v2.newBuilder()
                    .setId(42)
                    .setMessage(Message1v2.newBuilder().setId(42).build())
                    .build())
                    .build();
            final OuterV1 expected = OuterV1.newBuilder().setInner(Message2v1.newBuilder()
                    .setId(42)
                    .setMessage(Message1v1.newBuilder().setId(42).build())
                    .build())
                    .build();
            assertThat(OuterV1.parseFrom(v2OnlyId.toByteString())).isEqualTo(expected);
        }

        {
            final OuterV2 v2 = OuterV2.newBuilder().setInner(Message2v2.newBuilder()
                    .setId(42)
                    .setMessage(Message1v2.newBuilder()
                            .setId(42)
                            .setName("Foo Bar")
                            .build())
                    .build())
                    .build();

            final OuterV1 v1 = OuterV1.parseFrom(v2.toByteString());
            assertThat(v1.getUnknownFields().isEmpty()).isTrue();
            assertThat(v1.getInner().getUnknownFields().isEmpty()).isTrue();
            assertThat(v1.getInner().getMessage().getUnknownFields().isEmpty()).isFalse();
            assertThat(v1.getInner().getMessage().getUnknownFields().hasField(Message1v2.NAME_FIELD_NUMBER)).isTrue();

            // v2 is fine
            GrpcErrorHelper.checkHasNoUnknownFields(v2);
            GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v2);

            // v1 has no unknown fields _itself_
            GrpcErrorHelper.checkHasNoUnknownFields(v1);
            // v1.inner has no known fields _itself_
            GrpcErrorHelper.checkHasNoUnknownFields(v1.getInner());

            // but, recursively, it does have known fields
            try {
                GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v1);
                failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
            } catch (final StatusRuntimeException e) {
                assertThat(e).hasMessageContaining(
                        "io.deephaven.protobuf.test.Message1v1 has unknown field(s) with id [2], topLevel=io.deephaven.protobuf.test.OuterV1, path=[io.deephaven.protobuf.test.OuterV1.inner,io.deephaven.protobuf.test.Message2v1.message]");
            }
        }
    }

    @Test
    public void unknownFieldsRepeatedMessage() throws InvalidProtocolBufferException {
        {
            // establishing a precondition that we can decode just an id
            final RepeatedFieldv2 v2OnlyId = RepeatedFieldv2.newBuilder()
                    .addMessage(Message1v2.newBuilder().setId(42).build())
                    .build();
            final RepeatedFieldv1 expected = RepeatedFieldv1.newBuilder()
                    .addMessage(Message1v1.newBuilder().setId(42).build())
                    .build();
            assertThat(RepeatedFieldv1.parseFrom(v2OnlyId.toByteString())).isEqualTo(expected);
        }

        {
            final RepeatedFieldv2 v2 = RepeatedFieldv2.newBuilder()
                    .addMessage(Message1v2.newBuilder()
                            .setId(42)
                            .setName("Foo Bar")
                            .build())
                    .build();

            final RepeatedFieldv1 v1 = RepeatedFieldv1.parseFrom(v2.toByteString());
            assertThat(v1.getUnknownFields().isEmpty()).isTrue();
            for (Message1v1 message1v1 : v1.getMessageList()) {
                assertThat(message1v1.getUnknownFields().isEmpty()).isFalse();
                assertThat(message1v1.getUnknownFields().hasField(Message1v2.NAME_FIELD_NUMBER)).isTrue();
            }
            // v2 is fine
            GrpcErrorHelper.checkHasNoUnknownFields(v2);
            GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v2);

            // v1 no has unknown fields _itself_
            GrpcErrorHelper.checkHasNoUnknownFields(v1);
            // but, recursively, it does have known fields
            try {
                GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(v1);
                failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
            } catch (final StatusRuntimeException e) {
                assertThat(e).hasMessageContaining(
                        "io.deephaven.protobuf.test.Message1v1 has unknown field(s) with id [2], topLevel=io.deephaven.protobuf.test.RepeatedFieldv1, path=[io.deephaven.protobuf.test.RepeatedFieldv1.message]");
            }
        }
    }
}
