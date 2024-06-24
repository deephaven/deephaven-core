//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import io.deephaven.json.AnyValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;

final class AnyMixin extends GenericObjectMixin<AnyValue, TreeNode> {
    public AnyMixin(AnyValue options, JsonFactory factory) {
        super(factory, options, Type.ofCustom(TreeNode.class));
    }

    @Override
    public TreeNode parseValue(JsonParser parser) throws IOException {
        return parser.readValueAsTree();
    }

    @Override
    public TreeNode parseMissing(JsonParser parser) throws IOException {
        return parser.getCodec().missingNode();
    }
}
