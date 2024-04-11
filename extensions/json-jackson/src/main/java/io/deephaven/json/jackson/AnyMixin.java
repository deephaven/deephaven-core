//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.AnyOptions;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class AnyMixin extends Mixin<AnyOptions> {
    public AnyMixin(AnyOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
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
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return ObjectValueProcessor.of(out.get(0).asWritableObjectChunk(), ToTreeNode.INSTANCE);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new RepeaterGenericImpl<>(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull, null,
                null, ToTreeNode.INSTANCE, TreeNode.class, TreeNode[].class);
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
