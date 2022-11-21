package io.deephaven.server.table.ops;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.grpc.StatusRuntimeException;

class GrpcErrorHelper {
    static void checkHasField(Message message, int fieldNumber) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumber);
        if (!message.hasField(fieldDescriptor)) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, String.format("%s must have field %s (%d)",
                    descriptor.getFullName(), fieldDescriptor.getName(), fieldNumber));
        }
    }

    static void checkRepeatedFieldNonEmpty(Message message, int fieldNumber) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumber);
        if (!fieldDescriptor.isRepeated()) {
            throw new IllegalStateException(String.format(
                    "Should only be calling checkRepeatedFieldNonEmpty against a repeated field. %s %s (%d)",
                    descriptor.getFullName(), fieldDescriptor.getName(), fieldNumber));
        }
        if (message.getRepeatedFieldCount(fieldDescriptor) <= 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    String.format("%s must have at least one %s (%d)", descriptor.getFullName(),
                            fieldDescriptor.getName(), fieldNumber));
        }
    }

    static void checkHasOneOf(Message message, String oneOfName) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final OneofDescriptor oneofDescriptor =
                descriptor.getOneofs().stream().filter(o -> oneOfName.equals(o.getName())).findFirst().orElseThrow();
        if (!message.hasOneof(oneofDescriptor)) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "%s must have oneof %s. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.",
                    descriptor.getFullName(), oneOfName));
        }
    }
}
