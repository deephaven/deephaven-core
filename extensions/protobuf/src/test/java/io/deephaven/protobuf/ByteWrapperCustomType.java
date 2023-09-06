package io.deephaven.protobuf;

import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.protobuf.test.ByteWrapper;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.TypedFunction;

@AutoService(MessageParserSingle.class)
public class ByteWrapperCustomType implements MessageParserSingle {
    public ByteWrapperCustomType() {}

    @Override
    public Descriptor canonicalDescriptor() {
        return ByteWrapper.getDescriptor();
    }

    @Override
    public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
            FieldPath fieldPath) {
        final FieldDescriptor field = descriptor.findFieldByNumber(ByteWrapper.VALUE_FIELD_NUMBER);
        return (ToByteFunction<Message>) value -> (byte) (int) value.getField(field);
    }
}
