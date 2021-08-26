package io.deephaven.ide.shared;

import elemental2.core.JsArray;
import io.deephaven.web.shared.ide.lsp.CompletionItem;
import io.deephaven.web.shared.ide.lsp.DocumentRange;
import io.deephaven.web.shared.ide.lsp.Position;
import io.deephaven.web.shared.ide.lsp.TextEdit;

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
            io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CompletionItem src) {
        final CompletionItem item = new CompletionItem();
        item.setStart((int) src.getStart());
        item.setLength((int) src.getLength());
        item.setCommitCharacters(src.getCommitCharactersList());
        item.textEdit = toJs(src.getTextEdit());
        item.label = src.getLabel();
        item.deprecated = src.getDeprecated();
        item.preselect = src.getPreselect();
        if (!src.getDetail().isEmpty()) {
            item.detail = src.getDetail();
        }
        if (!src.getDocumentation().isEmpty()) {
            item.documentation = src.getDocumentation();
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
        final JsArray<io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.TextEdit> textEdits =
                src.getAdditionalTextEditsList();
        for (int i = 0; i < textEdits.getLength(); i++) {
            edits.push(toJs(textEdits.getAt(i)));
        }
        item.setAdditionalTextEdits(edits);
        return item;
    }

    private static TextEdit toJs(
            final io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.TextEdit src) {
        final TextEdit item = new TextEdit();
        item.text = src.getText();
        item.range = toJs(src.getRange());
        return item;
    }

    private static DocumentRange toJs(
            final io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.DocumentRange range) {
        final DocumentRange item = new DocumentRange();
        item.start = toJs(range.getStart());
        item.end = toJs(range.getEnd());
        return item;
    }

    private static Position toJs(
            final io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.Position src) {
        final Position item = new Position();
        item.line = (int) src.getLine();
        item.character = (int) src.getCharacter();
        return item;
    }
}
