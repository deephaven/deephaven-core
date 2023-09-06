package io.deephaven.protobuf;

import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.protobuf.test.ByteWrapper;
import io.deephaven.qst.type.Type;

@AutoService(MessageParser.class)
public class ByteWrapperCustomType implements MessageParserSingle {
    private static final ToByteFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
            .<Message, ByteWrapper>identity(Type.ofCustom(ByteWrapper.class))
            .mapToByte(ByteWrapperCustomType::getByte);

    public ByteWrapperCustomType() {}

    @Override
    public Descriptor canonicalDescriptor() {
        return ByteWrapper.getDescriptor();
    }

    @Override
    public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
            FieldPath fieldPath) {
        return CANONICAL_FUNCTION;
    }

    private static byte getByte(ByteWrapper wrapper) {
        return (byte) wrapper.getValue();
    }
}
