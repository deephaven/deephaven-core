//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import io.deephaven.json.AnyValue;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class AnyMixin extends Mixin<AnyValue> {
    public AnyMixin(AnyValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int outputSize() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.of(Type.ofCustom(TreeNode.class));
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ObjectValueProcessor<>(ToTreeNode.INSTANCE, Type.ofCustom(TreeNode.class));
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new RepeaterGenericImpl<>(ToTreeNode.INSTANCE, allowMissing, allowNull, null, null,
                Type.ofCustom(TreeNode.class).arrayType());
    }

    private enum ToTreeNode implements ToObject<TreeNode> {
        INSTANCE;

        @Override
        public TreeNode parseValue(JsonParser parser) throws IOException {
            return parser.readValueAsTree();
        }

        @Override
        public TreeNode parseMissing(JsonParser parser) {
            return parser.getCodec().missingNode();
        }
    }
}
