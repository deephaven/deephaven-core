package io.deephaven.lang.parse.api;

import io.deephaven.io.logger.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A do-nothing CompletionParseService, to be used only if the real completer is not available.
 */
public class CompletionParseServiceNoOp implements CompletionParseService<ParsedResult<Object, Object, Object>, Object, RuntimeException> {

    @Override
    public ParsedResult<Object, Object, Object> parse(final String document) throws RuntimeException {
        return new ParsedResult<Object, Object, Object>() {
            private Map<String, List<Object>> map = new LinkedHashMap<>();

            @Override
            public Object findNode(final int p) {
                return null;
            }

            @Override
            public Object getDoc() {
                return null;
            }

            @Override
            public String getSource() {
                return document;
            }

            @Override
            public Map<String, List<Object>> getAssignments() {
                return map;
            }
        };
    }

    @Override
    public void open(final String text, final String uri, final String version) {

    }

    @Override
    public void update(final String uri, final String version, final List<Object> changes) {

    }

    @Override
    public void remove(final String uri) {

    }

    @Override
    public ParsedResult<Object, Object, Object> finish(final String uri) {
        return parse("");
    }

    @Override
    public void close(final String uri) {

    }

    @Override
    public void setLanguage(final String language) {
    }

}
