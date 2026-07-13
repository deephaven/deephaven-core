//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import elemental2.core.JsArray;
import io.deephaven.proto.backplane.script.grpc.GetHoverResponse;
import io.deephaven.web.shared.ide.lsp.*;

/**
 * LspTranslate:
 * <p>
 * <p>
 * This class is responsible for transforming "off-the-wire" protobuf completion responses into js-friendly "js api"
 * objects that we can hand off to clients.
 * <p>
 * <p>
 */
public class LspTranslate {

    public static CompletionItem toJs(
            io.deephaven.proto.backplane.script.grpc.CompletionItem src) {
        final CompletionItem item = new CompletionItem();
        item.setStart(src.getStart());
        item.setLength(src.getLength());
        item.commitCharacters = JsArray.from(src.getCommitCharactersList().toArray(new String[0]));
        item.textEdit = toJs(src.getTextEdit());
        item.label = src.getLabel();
        item.deprecated = src.getDeprecated();
        item.preselect = src.getPreselect();
        if (!src.getDetail().isEmpty()) {
            item.detail = src.getDetail();
        }
        if (src.hasDocumentation()) {
            item.documentation = toJs(src.getDocumentation());
        }
        if (!src.getFilterText().isEmpty()) {
            item.filterText = src.getFilterText();
        }
        if (!src.getSortText().isEmpty()) {
            item.sortText(src.getSortText());
        }
        if (src.getKind() != 0) {
            item.kind = (int) src.getKind();
        }
        if (src.getInsertTextFormat() != 0) {
            item.insertTextFormat = (int) src.getInsertTextFormat();
        }

        final JsArray<TextEdit> edits = new JsArray<>();
        for (int i = 0; i < src.getAdditionalTextEditsCount(); i++) {
            edits.push(toJs(src.getAdditionalTextEdits(i)));
        }
        item.additionalTextEdits = edits;
        return item;
    }

    private static TextEdit toJs(
            final io.deephaven.proto.backplane.script.grpc.TextEdit src) {
        final TextEdit item = new TextEdit();
        item.text = src.getText();
        item.range = toJs(src.getRange());
        return item;
    }

    private static DocumentRange toJs(
            final io.deephaven.proto.backplane.script.grpc.DocumentRange range) {
        final DocumentRange item = new DocumentRange();
        item.start = toJs(range.getStart());
        item.end = toJs(range.getEnd());
        return item;
    }

    private static Position toJs(
            final io.deephaven.proto.backplane.script.grpc.Position src) {
        final Position item = new Position();
        item.line = (int) src.getLine();
        item.character = (int) src.getCharacter();
        return item;
    }

    private static MarkupContent toJs(
            final io.deephaven.proto.backplane.script.grpc.MarkupContent src) {
        final MarkupContent content = new MarkupContent();

        content.kind = src.getKind();
        content.value = src.getValue();
        return content;
    }

    public static SignatureInformation toJs(
            io.deephaven.proto.backplane.script.grpc.SignatureInformation src) {
        final SignatureInformation item = new SignatureInformation();
        item.label = src.getLabel();
        if (src.hasDocumentation()) {
            item.documentation = toJs(src.getDocumentation());
        }
        if (src.hasActiveParameter()) {
            item.activeParameter = src.getActiveParameter();
        }

        final JsArray<ParameterInformation> params = new JsArray<>();
        for (int i = 0; i < src.getParametersCount(); i++) {
            params.push(toJs(src.getParameters(i)));
        }
        item.parameters = params;
        return item;
    }

    private static ParameterInformation toJs(
            final io.deephaven.proto.backplane.script.grpc.ParameterInformation src) {
        final ParameterInformation item = new ParameterInformation();
        item.label = src.getLabel();
        item.documentation = toJs(src.getDocumentation());
        return item;
    }

    public static Hover toJs(
            final GetHoverResponse src) {
        final Hover item = new Hover();

        if (src.hasContents()) {
            item.contents = toJs(src.getContents());
        }
        if (src.hasRange()) {
            item.range = toJs(src.getRange());
        }

        return item;
    }
}
