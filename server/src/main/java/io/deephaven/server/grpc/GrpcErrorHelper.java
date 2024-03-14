//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.grpc;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import io.grpc.StatusRuntimeException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map.Entry;
import java.util.Objects;

public class GrpcErrorHelper {
    public static void checkHasField(Message message, int fieldNumber) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumber);
        if (!message.hasField(fieldDescriptor)) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format("%s must have field %s (%d)",
                    descriptor.getFullName(), fieldDescriptor.getName(), fieldNumber));
        }
    }

    public static void checkDoesNotHaveField(Message message, int fieldNumber) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumber);
        if (message.hasField(fieldDescriptor)) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    String.format("%s must not have field %s (%d)",
                            descriptor.getFullName(), fieldDescriptor.getName(), fieldNumber));
        }
    }

    public static void checkRepeatedFieldNonEmpty(Message message, int fieldNumber) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(fieldNumber);
        if (!fieldDescriptor.isRepeated()) {
            throw new IllegalStateException(String.format(
                    "Should only be calling checkRepeatedFieldNonEmpty against a repeated field. %s %s (%d)",
                    descriptor.getFullName(), fieldDescriptor.getName(), fieldNumber));
        }
        if (message.getRepeatedFieldCount(fieldDescriptor) <= 0) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    String.format("%s must have at least one %s (%d)", descriptor.getFullName(),
                            fieldDescriptor.getName(), fieldNumber));
        }
    }

    public static void checkHasOneOf(Message message, String oneOfName) throws StatusRuntimeException {
        final Descriptor descriptor = message.getDescriptorForType();
        final OneofDescriptor oneofDescriptor =
                descriptor.getOneofs().stream().filter(o -> oneOfName.equals(o.getName())).findFirst().orElseThrow();
        if (!message.hasOneof(oneofDescriptor)) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "%s must have oneof %s. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.",
                    descriptor.getFullName(), oneOfName));
        }
    }

    public static void checkHasNoUnknownFields(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    String.format("%s has unknown field(s)", message.getDescriptorForType().getFullName()));
        }
    }

    public static void checkHasNoUnknownFieldsRecursive(Message message) {
        checkHasNoUnknownFields(message);
        for (Entry<FieldDescriptor, Object> e : message.getAllFields().entrySet()) {
            if (e instanceof Message) {
                checkHasNoUnknownFieldsRecursive((Message) e);
            }
        }
    }

    public static FieldDescriptor extractField(Descriptor desc, int fieldNumber, Class<? extends Message> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final FieldDescriptor field = desc.findFieldByNumber(fieldNumber);
        final Descriptor messageType = field.getMessageType();
        final Method method = clazz.getDeclaredMethod("getDescriptor");
        final Object results = method.invoke(null);
        if (!Objects.equals(messageType, results)) {
            throw new IllegalStateException(String.format("Types don't match, %s, %s", field.getName(), clazz));
        }
        return field;
    }
}
